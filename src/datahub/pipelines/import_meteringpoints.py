"""
Asynchronous tasks for importing MeteringPoints from ElOverblik.

One entrypoint exists:

    start_import_meteringpoints_pipeline()

"""
from celery import group, chain
from sqlalchemy import orm

from datahub import logger
from datahub.tasks import celery_app
from datahub.db import inject_session, atomic
from datahub.services.eloverblik import EloverblikService
from datahub.meteringpoints import MeteringPointImporter
from datahub.services.energytypes.service import EnergyTypeUnavailable
from datahub.webhooks import (
    WebhookEvent,
    WebhookService,
    WebhookError,
    WebhookConnectionError,
)


# Settings
RETRY_DELAY = 60
MAX_RETRIES = (24 * 60 * 60) / RETRY_DELAY


# Services
service = EloverblikService()
importer = MeteringPointImporter()
webhook = WebhookService()


def start_import_meteringpoints_pipeline(subject, session):
    """
    Starts a pipeline which imports meteringpoints for a specific subject.

        Step 1: import_meteringpoints()  imports meteringpoints for
                a specific subject

        Step 2: Invokes the "METERINGPOINTS AVAILABLE" webhook

    :param str subject:
    :param sqlalchemy.orm.Session session:
    """
    tasks = [
        import_meteringpoints.s(subject=subject),
    ]

    subscriptions = webhook.get_subscriptions(
        event=WebhookEvent.ON_METERINGPOINTS_AVAILABLE,
        subject=subject,
        session=session,
    )

    if subscriptions:
        tasks.append(group(
            invoke_webhook.si(
                subject=subject,
                subscription_id=subscription.id,
            )
            for subscription in subscriptions
        ))

    chain(*tasks).apply_async()


@celery_app.task(
    bind=True,
    name='import_meteringpoints.import_meteringpoints',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Importing MeteringPoints from ElOverblik',
    pipeline='import_meteringpoints',
    task='import_meteringpoints',
)
def import_meteringpoints(task, subject):
    """
    Imports meteringpoints for a specific subject

    :param celery.Task task:
    :param str subject:
    """
    __log_extra = {
        'subject': subject,
        'pipeline': 'import_meteringpoints',
        'task': 'import_meteringpoints',
    }

    @atomic
    def __import_meteringpoints(session):
        """
        Import and save to DB as an atomic operation
        """
        importer.import_meteringpoints(subject, session)

    try:
        __import_meteringpoints()
    except EnergyTypeUnavailable:
        raise
    except Exception as e:
        logger.exception('Failed to import meteringpoints, retrying...', extra=__log_extra)
        raise task.retry(exc=e)


@celery_app.task(
    bind=True,
    name='import_meteringpoints.invoke_webhook',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Invoking webhook ON_METERINGPOINTS_AVAILABLE (subscription ID: %(subscription_id)d)',
    pipeline='import_meteringpoints',
    task='invoke_webhook',
)
@inject_session
def invoke_webhook(task, subject, subscription_id, session):
    """
    :param celery.Task task:
    :param str subject:
    :param int subscription_id:
    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'subject': subject,
        'subscription_id': str(subscription_id),
        'pipeline': 'compose',
        'task': 'invoke_webhook',
    }

    # Get webhook subscription from database
    try:
        subscription = webhook.get_subscription(subscription_id, session)
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load WebhookSubscription from database', extra=__log_extra)
        raise task.retry(exc=e)

    # Publish event to webhook
    try:
        webhook.on_meteringpoints_available(subscription)
    except WebhookConnectionError as e:
        logger.exception('Failed to invoke webhook: ON_METERINGPOINTS_AVAILABLE (Connection error)', extra=__log_extra)
        raise task.retry(exc=e)
    except WebhookError as e:
        logger.exception('Failed to invoke webhook: ON_METERINGPOINTS_AVAILABLE', extra=__log_extra)
        raise task.retry(exc=e)
