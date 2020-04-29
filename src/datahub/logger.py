import logging
from functools import partial, wraps
from opencensus.ext.azure.log_exporter import AzureLogHandler

from .tasks import Retry
from .settings import PROJECT_NAME, AZURE_APP_INSIGHTS_CONN_STRING


logger = logging.getLogger(PROJECT_NAME)


if AZURE_APP_INSIGHTS_CONN_STRING:
    print('Exporting logs to Azure Application Insight', flush=True)

    logger.addHandler(AzureLogHandler(
        connection_string=AZURE_APP_INSIGHTS_CONN_STRING,
        export_interval=5.0,
    ))

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
    error = logger.error
    critical = logger.critical
    warning = logger.warning
    info = logger.info
    debug = logger.debug
    exception = logger.exception


def wrap_task(title, pipeline, task):
    def wrap_task_decorator(function):
        """
        A decorator that wraps the passed in function and logs
        exceptions should one occur
        """

        @wraps(function)
        def wrap_task_wrapper(*args, **kwargs):
            formatted_title = title % kwargs
            extra = kwargs.copy()
            extra.update({
                'title': formatted_title,
                'task': task,
                'pipeline': pipeline,
                'args': str(args),
                'kwargs': str(kwargs),
            })

            info(f'Task: {formatted_title}', extra=extra)

            try:
                return function(*args, **kwargs)
            except Retry:
                raise
            except:
                exception(f'Task resulted in an exception: {formatted_title}', extra=extra)
                raise

        return wrap_task_wrapper
    return wrap_task_decorator
