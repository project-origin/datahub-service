"""
TODO write this
"""
import logging

from datahub.tasks import celery_app
from datahub.db import inject_session
from datahub.webhooks import WebhookService
from datahub.services.eloverblik import EloverblikService
from datahub.meteringpoints import MeteringPointsImportController


service = EloverblikService()
importer = MeteringPointsImportController()
webhook = WebhookService()


def start_import_meteringpoints_pipeline(sub):
    """
    TODO

    :param str sub:
    """
    import_meteringpoints.s(sub) \
        .apply_async()


@celery_app.task(name='import_meteringpoints.import_meteringpoints')
@inject_session
def import_meteringpoints(sub, session):
    """
    :param str sub:
    :param Session session:
    """
    logging.info('--- import_meteringpoints.import_meteringpoints, sub=%s' % sub)

    importer.import_meteringpoints(sub)

    invoke_webhook.s(sub).apply_async()


@celery_app.task(name='import_meteringpoints.invoke_webhook')
def invoke_webhook(sub):
    """
    :param str sub:
    """
    logging.info('--- import_meteringpoints.invoke_webhook, sub=%s' % sub)

    webhook.on_meteringpoints_available(sub)
