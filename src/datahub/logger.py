import logging
from functools import partial, wraps
from opencensus.trace.tracer import Tracer
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.ext.azure.log_exporter import AzureLogHandler

from .tasks import Task, Retry
from .settings import SERVICE_NAME, AZURE_APP_INSIGHTS_CONN_STRING, LOG_LEVEL


logger = logging.getLogger(SERVICE_NAME)
handler = None
exporter = None
sampler = None


if AZURE_APP_INSIGHTS_CONN_STRING:
    print('Exporting logs to Azure Application Insight', flush=True)

    def __telemetry_processor(envelope):
        envelope.data.baseData.cloud_roleName = SERVICE_NAME
        envelope.tags['ai.cloud.role'] = SERVICE_NAME

    handler = AzureLogHandler(
        connection_string=AZURE_APP_INSIGHTS_CONN_STRING,
        export_interval=5.0,
    )
    handler.add_telemetry_processor(__telemetry_processor)
    logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)

    exporter = AzureExporter(connection_string=AZURE_APP_INSIGHTS_CONN_STRING)
    exporter.add_telemetry_processor(__telemetry_processor)

    sampler = AlwaysOnSampler()
    tracer = Tracer(exporter=exporter, sampler=sampler)

    def __route_extras_to_azure(f, *args, extra=None, **kwargs):
        if extra is None:
            extra = {}
        extra['project'] = SERVICE_NAME
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


def wrap_task(pipeline, task, title):
    def wrap_task_decorator(function):
        """
        A decorator that wraps the passed in function and logs
        exceptions should one occur
        """

        @wraps(function)
        def wrap_task_wrapper(*args, **kwargs):
            formatted_title = 'Task: %s (args: %s, kwargs: %s)' % (
                (title % kwargs),
                str(tuple(a for a in args if not isinstance(a, Task))),
                str(kwargs),
            )

            extra = kwargs.copy()
            extra.update({
                'task': task,
                'formatted_title': formatted_title,
                'pipeline': pipeline,
                'task_args': str(args),
                'task_kwargs': str(kwargs),
            })

            info(formatted_title, extra=extra)

            try:
                with tracer.span(formatted_title):
                    return function(*args, **kwargs)
            except Retry:
                raise
            except:
                exception(f'Task resulted in an exception', extra=extra)
                raise

        return wrap_task_wrapper
    return wrap_task_decorator
