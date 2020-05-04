"""
TODO write this
"""
from datahub import logger
from datahub.tasks import celery_app
from datahub.webhooks import WebhookService
from datahub.services.eloverblik import EloverblikService
from datahub.meteringpoints import MeteringPointsImportController


service = EloverblikService()
importer = MeteringPointsImportController()
webhook = WebhookService()


def start_compile_disclosure_pipeline(subject):
    """
    TODO

    :param str subject:
    """
    import_meteringpoints \
        .s(subject=subject) \
        .apply_async()


@celery_app.task(
    name='disclosure.import_meteringpoints',
    queue='disclosure',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Importing MeteringPoints from ElOverblik',
    pipeline='import_meteringpoints',
    task='import_meteringpoints',
)
def import_meteringpoints(subject):
    """
    :param str subject:
    """
    importer.import_meteringpoints(subject)

    invoke_webhook.s(subject=subject).apply_async()


@celery_app.task(
    name='import_meteringpoints.invoke_webhook',
    queue='import-meteringpoints',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Invoking webhooks: on_meteringpoints_available',
    pipeline='import_meteringpoints',
    task='invoke_webhook',
)
def invoke_webhook(subject):
    """
    :param str subject:
    """
    webhook.on_meteringpoints_available(subject)
