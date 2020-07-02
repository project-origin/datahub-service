"""
Asynchronous tasks for importing MeteringPoints from ElOverblik.

One entrypoint exists:

    start_import_meteringpoints_pipeline()

"""
from sqlalchemy import orm
from celery import group, chain, shared_task

from datahub import logger
from datahub.db import inject_session, atomic
from datahub.meteringpoints import MeteringPointImporter, MeteringPointQuery
from datahub.services.energytypes import (
    EnergyTypeService,
    EnergyTypeUnavailable,
)
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
importer = MeteringPointImporter()
energtype_service = EnergyTypeService()
webhook_service = WebhookService()


def start_import_meteringpoints_pipeline(subject):
    """
    Starts a pipeline which imports meteringpoints for a specific subject.

    :param str subject:
    """
    import_meteringpoints.s(subject=subject).apply_async()


def start_import_energy_type_pipeline(subject, gsrn, session):
    """
    Starts a pipeline which imports energy type for a specific metering point
    and then invokes ON_METERINGPOINT_AVAILABLE webhook.

    :param str subject:
    :param str gsrn:
    :param sqlalchemy.orm.Session session:
    """
    chain(
        import_energy_type.si(subject=subject, gsrn=gsrn),
        build_on_meteringpoint_available_webhooks(subject, gsrn, session),
    ).apply_async()


def start_on_meteringpoint_available_webhooks(*args, **kwargs):
    build_on_meteringpoint_available_webhooks(*args, **kwargs).apply_async()


def build_on_meteringpoint_available_webhooks(subject, gsrn, session):
    """
    :param str subject:
    :param str gsrn:
    :param sqlalchemy.orm.Session session:
    :rtype: celery.group
    """
    subscriptions = webhook_service.get_subscriptions(
        event=WebhookEvent.ON_METERINGPOINT_AVAILABLE,
        subject=subject,
        session=session,
    )

    return group(
        invoke_webhook.si(
            subject=subject,
            gsrn=gsrn,
            subscription_id=subscription.id,
        )
        for subscription in subscriptions
    )


@shared_task(
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
@inject_session
def import_meteringpoints(task, subject, session):
    """
    Imports meteringpoints for a specific subject

    :param celery.Task task:
    :param str subject:
    :param sqlalchemy.orm.Session session:
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
        return importer.import_meteringpoints(subject, session)

    # Import MeteringPoints
    try:
        meteringpoints = __import_meteringpoints()
    except Exception as e:
        logger.exception('Failed to import meteringpoints, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Start tasks to complete pipeline
    # If PRODUCTION, first import energy type then invoke webhooks
    # If CONSUMPTION, just invoke webhooks
    for meteringpoint in meteringpoints:
        if meteringpoint.is_producer():
            start_import_energy_type_pipeline(
                subject, meteringpoint.gsrn, session)
        else:
            start_on_meteringpoint_available_webhooks(
                subject, meteringpoint.gsrn, session)


@shared_task(
    bind=True,
    name='import_meteringpoints.import_energy_type',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Importing EnergyType from EnergyTypeService',
    pipeline='import_meteringpoints',
    task='import_energy_type',
)
@atomic
def import_energy_type(task, subject, gsrn, session):
    """
    Imports meteringpoints for a specific subject

    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'subject': subject,
        'gsrn': gsrn,
        'pipeline': 'import_meteringpoints',
        'task': 'import_energy_type',
    }

    # Get MeteringPoint from DB
    try:
        meteringpoint = MeteringPointQuery(session) \
            .has_gsrn(gsrn) \
            .is_production() \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load MeteringPoint from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Import energy type from EnergyTypeService
    try:
        technology_code, fuel_code = energtype_service.get_energy_type(gsrn)
    except EnergyTypeUnavailable:
        raise
    except Exception as e:
        logger.exception('Failed to import energy type, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Update energy type in DB
    meteringpoint.technology_code = technology_code
    meteringpoint.fuel_code = fuel_code


@shared_task(
    bind=True,
    name='import_meteringpoints.invoke_webhook',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Invoking webhook ON_METERINGPOINT_AVAILABLE (subscription ID: %(subscription_id)d)',
    pipeline='import_meteringpoints',
    task='invoke_webhook',
)
@inject_session
def invoke_webhook(task, subject, gsrn, subscription_id, session):
    """
    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param int subscription_id:
    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'subject': subject,
        'gsrn': gsrn,
        'subscription_id': str(subscription_id),
        'pipeline': 'compose',
        'task': 'invoke_webhook',
    }

    # Get MeteringPoint from DB
    try:
        meteringpoint = MeteringPointQuery(session) \
            .has_gsrn(gsrn) \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load MeteringPoint from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Get webhook subscription from database
    try:
        subscription = webhook_service.get_subscription(subscription_id, session)
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load WebhookSubscription from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Publish event to webhook
    try:
        webhook_service.on_meteringpoint_available(subscription, meteringpoint)
    except WebhookConnectionError as e:
        logger.exception('Failed to invoke webhook: ON_METERINGPOINT_AVAILABLE (Connection error), retrying...', extra=__log_extra)
        raise task.retry(exc=e)
    except WebhookError as e:
        logger.exception('Failed to invoke webhook: ON_METERINGPOINT_AVAILABLE, retrying...', extra=__log_extra)
        raise task.retry(exc=e)
