import logging
from functools import partial, wraps
from opencensus.trace.tracer import Tracer
from opencensus.trace.samplers import ProbabilitySampler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.ext.azure.log_exporter import AzureLogHandler

from .tasks import Retry
from .settings import PROJECT_NAME, AZURE_APP_INSIGHTS_CONN_STRING


logger = logging.getLogger(PROJECT_NAME)
handler = None
exporter = None
sampler = None


if AZURE_APP_INSIGHTS_CONN_STRING:
    print('Exporting logs to Azure Application Insight', flush=True)

    def __telemetry_processor(envelope):
        envelope.data.baseData.cloud_roleName = PROJECT_NAME
        envelope.tags['ai.cloud.role'] = PROJECT_NAME

    handler = AzureLogHandler(
        connection_string=AZURE_APP_INSIGHTS_CONN_STRING,
        export_interval=5.0,
    )
    handler.add_telemetry_processor(__telemetry_processor)
    logger.addHandler(handler)

    exporter = AzureExporter(connection_string=AZURE_APP_INSIGHTS_CONN_STRING)
    exporter.add_telemetry_processor(__telemetry_processor)

    sampler = ProbabilitySampler(1.0)
    tracer = Tracer(exporter=exporter, sampler=sampler)

    def __route_extras_to_azure(f, *args, extra=None, **kwargs):
        if extra is None:
            extra = {}
        extra['project'] = PROJECT_NAME
        actual_extra = {'custom_dimensions': extra}
        return f(*args, extra=actual_extra, **kwargs)

    error = partial(__route_extras_to_azure, logger.error)
    critical = partial(__route_extras_to_azure, logger.critical)
    warning = partial(__route_extras_to_azure, logger.warning)
    info = partial(__route_extras_to_azure, logger.info)
    debug = partial(__route_extras_to_azure, logger.debug)
    exception = partial(__route_extras_to_azure, logger.exception)
else:
    tracer = Tracer()
    error = logger.error
    critical = logger.critical
    warning = logger.warning
    info = logger.info
    debug = logger.debug
    exception = logger.exception


def wrap_task(pipeline, task, title=None):
    def wrap_task_decorator(function):
        """
        A decorator that wraps the passed in function and logs
        exceptions should one occur
        """

        @wraps(function)
        def wrap_task_wrapper(*args, **kwargs):
            extra = kwargs.copy()
            extra.update({
                'task': task,
                'pipeline': pipeline,
                'task_args': str(args),
                'task_kwargs': str(kwargs),
            })
            if title:
                formatted_title = title % kwargs
                info(f'Task: {formatted_title}', extra=extra)

            try:
                return function(*args, **kwargs)
            except Retry:
                raise
            except:
                exception(f'Task resulted in an exception', extra=extra)
                raise

        return wrap_task_wrapper
    return wrap_task_decorator
