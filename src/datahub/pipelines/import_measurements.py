"""
Asynchronous tasks for importing Measurements from ElOverblik.

Multiple entrypoints exists depending on the use case:

    1) Function start_import_measurements_pipeline() starts a pipeline which
       imports measurements for all MeteringPoints in the database

    2) Function start_import_measurements_pipeline_for() starts a pipeline
       which imports measurements for a single MeteringPoint.

    3) Function start_submit_measurement_pipeline() starts a pipelines which
       submits a single measurement to the ledger.
"""
import origin_ledger_sdk as ols
from celery import group, chain
from sqlalchemy import orm

from datahub import logger
from datahub.tasks import celery_app
from datahub.db import atomic, inject_session
from datahub.services.eloverblik import EloverblikService
from datahub.settings import LEDGER_URL, DEBUG, BATCH_RESUBMIT_AFTER_HOURS
from datahub.meteringpoints import (
    MeteringPointQuery,
    MeasurementType,
    MeteringPoint,
)
from datahub.webhooks import (
    WebhookEvent,
    WebhookService,
    WebhookError,
    WebhookConnectionError,
)
from datahub.measurements import (
    Measurement,
    MeasurementQuery,
    MeasurementImportController,
)


# Settings
SUBMIT_RETRY_DELAY = 20
SUBMIT_MAX_RETRIES = (BATCH_RESUBMIT_AFTER_HOURS * 60 * 60) / SUBMIT_RETRY_DELAY

POLL_RETRY_DELAY = 10
POLL_MAX_RETRIES = (BATCH_RESUBMIT_AFTER_HOURS * 60 * 60) / POLL_RETRY_DELAY

WEBHOOK_RETRY_DELAY = 10
WEBHOOK_MAX_RETRIES = (24 * 60 * 60) / WEBHOOK_RETRY_DELAY


# Services
service = EloverblikService()
importer = MeasurementImportController()
webhook = WebhookService()
ledger = ols.Ledger(LEDGER_URL, verify=not DEBUG)


def start_import_measurements_pipeline():
    """
    Starts a pipeline which imports measurements for all
    MeteringPoints in the database.
    """
    get_distinct_gsrn \
        .s() \
        .apply_async()


def start_import_measurements_pipeline_for(subject, gsrn):
    """
    Starts a pipeline which imports measurements for a single MeteringPoint.

    :param str subject:
    :param str gsrn:
    """
    import_measurements \
        .s(subject=subject, gsrn=gsrn) \
        .apply_async()


def start_submit_measurement_pipeline(measurement, meteringpoint, session):
    """
    Starts a pipelines which submits a single measurement to the ledger.

        Step 1: submit_to_ledger() submits the Batch to the ledger,
                and returns the Handle, which is passed on to step 2

        Step 2: poll_batch_status() polls the ledger for Batch status
                until it has been completed/declined, and returns whether
                or not the batch was submitted successfully

        Step 3: update_measurement_status() updates the Measurement's
                "published" property

        Step 4: invoke_webhook() invokes the "GGO ISSUED" webhook ONLY
                if the Measurement has an associated GGO issued (production)

    :param Measurement measurement:
    :param MeteringPoint meteringpoint:
    :param sqlalchemy.orm.Session session:
    """
    build_submit_measurement_pipeline(measurement, meteringpoint, session) \
        .apply_async()


def build_submit_measurement_pipeline(measurement, meteringpoint, session):
    """
    Builds and returns TODO

    :param Measurement measurement:
    :param MeteringPoint meteringpoint:
    :param sqlalchemy.orm.Session session:
    """
    tasks = [
        # Submit Batch with Measurement (and Ggo if PRODUCTION)
        submit_to_ledger.si(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
            measurement_id=measurement.id,
        ),

        # Poll for Batch status
        poll_batch_status.s(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
            measurement_id=measurement.id,
        ),

        # Update Measurement.published status attribute
        update_measurement_status.si(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
            measurement_id=measurement.id,
        ),
    ]

    # If PRODUCTION, also invoke OnGgoIssued webhook
    if meteringpoint.type is MeasurementType.PRODUCTION:
        subscriptions = webhook.get_subscriptions(
            event=WebhookEvent.ON_GGOS_ISSUED,
            subject=meteringpoint.sub,
            session=session,
        )

        if subscriptions:
            tasks.append(group(
                invoke_webhook.si(
                    subject=meteringpoint.sub,
                    gsrn=meteringpoint.gsrn,
                    measurement_id=measurement.id,
                    subscription_id=subscription.id,
                )
                for subscription in subscriptions
            ))

    return chain(*tasks)


@celery_app.task(
    bind=True,
    name='import_measurements.get_distinct_gsrn',
    queue='import-measurements',
    default_retry_delay=5,
    max_retries=20,
)
@logger.wrap_task(
    title='Getting distinct GSRN numbers for importing measurements',
    pipeline='import_measurements',
    task='get_distinct_gsrn',
)
@inject_session
def get_distinct_gsrn(task, session):
    """
    Fetches all distinct GSRN numbers from the database, and starts a
    import_measurements() pipelines for each of them.

    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'pipeline': 'import_measurements',
        'task': 'get_distinct_gsrn',
    }

    try:
        meteringpoints = MeteringPointQuery(session).all()
    except Exception as e:
        logger.exception('Failed to load MeteringPoints from database', extra=__log_extra)
        raise task.retry(exc=e)

    tasks = [
        import_measurements.s(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
        )
        for meteringpoint in meteringpoints
    ]

    if tasks:
        group(tasks).apply_async()


@celery_app.task(
    bind=True,
    name='import_measurements.import_measurements',
    queue='import-measurements',
    default_retry_delay=10,
    max_retries=50,
)
@logger.wrap_task(
    title='Importing measurements for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='import_measurements',
)
@inject_session
def import_measurements(task, subject, gsrn, session):
    """
    Imports measurements for a single MeteringPoint, and starts a
    start_submit_measurement_pipeline() pipeline for each of the newly
    imported measurements.

    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'gsrn': gsrn,
        'subject': subject,
        'pipeline': 'import_measurements',
        'task': 'import_measurements',
    }

    @atomic
    def __import_measurements(session):
        """
        Import and save to DB as an atomic operation
        """
        return importer.import_measurements_for(meteringpoint, session)

    # Load MeteringPoint from DB
    try:
        meteringpoint = MeteringPointQuery(session) \
            .has_gsrn(gsrn) \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        raise task.retry(exc=e)

    # Import measurements into DB
    try:
        measurements = __import_measurements()
    except Exception as e:
        logger.exception('Failed to import measurements from ElOverblik', extra=__log_extra)
        raise task.retry(exc=e)

    # Submit each measurement to ledger in parallel
    if measurements:
        group([
            build_submit_measurement_pipeline(measurement, meteringpoint, session)
            for measurement in measurements
        ]).apply_async()


@celery_app.task(
    bind=True,
    name='import_measurements.submit_to_ledger',
    queue='import-measurements',
    default_retry_delay=SUBMIT_RETRY_DELAY,
    max_retries=SUBMIT_MAX_RETRIES,
)
@logger.wrap_task(
    title='Publishing measurement to ledger',
    pipeline='import_measurements',
    task='submit_to_ledger',
)
@atomic
def submit_to_ledger(task, subject, gsrn, measurement_id, session):
    """
    Submits a single measurement (and its associated GGO, if any) to the
    ledger. Returns the ledger Handle, which is passed on to the next chained
    task (poll_batch_status).

    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param int measurement_id:
    :param sqlalchemy.orm.Session session:
    :rtype: str
    """
    __log_extra = {
        'gsrn': gsrn,
        'subject': subject,
        'measurement_id': str(measurement_id),
        'pipeline': 'import_measurements',
        'task': 'submit_to_ledger',
    }

    # Get Measurement from DB
    try:
        measurement = MeasurementQuery(session) \
            .has_id(measurement_id) \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Measurement from database', extra=__log_extra)
        raise task.retry(exc=e)

    # Submit batch to ledger
    try:
        handle = ledger.execute_batch(measurement.build_batch())
    except ols.LedgerConnectionError as e:
        logger.exception('Failed to submit batch to ledger', extra=__log_extra)
        raise task.retry(exc=e)
    except ols.LedgerException as e:
        if e.code in (15, 17, 18):
            logger.exception(f'Ledger validator error (code {e.code}), retrying...', extra=__log_extra)
            raise task.retry(exc=e)
        elif e.code == 31:
            logger.info(f'Ledger queue is full, retrying...', extra=__log_extra)
            raise task.retry(exc=e)
        else:
            raise

    logger.info(f'Batch submitted to ledger for GSRN: {gsrn}', extra=__log_extra)

    # TODO move to a task for itself?
    measurement.set_submitted_to_ledger()

    return handle


@celery_app.task(
    bind=True,
    name='import_measurements.poll_batch_status',
    queue='import-measurements',
    default_retry_delay=POLL_RETRY_DELAY,
    max_retries=POLL_MAX_RETRIES,
)
@logger.wrap_task(
    title='Poll batch status',
    pipeline='import_measurements',
    task='poll_batch_status',
)
def poll_batch_status(task, handle, subject, gsrn, measurement_id):
    """
    Polls the ledger for Batch status until it has been completed/declined.
    The param "handle" is passed on by the previous task (submit_to_ledger).

    :param celery.Task task:
    :param str handle:
    :param str subject:
    :param str gsrn:
    :param int measurement_id:
    """
    __log_extra = {
        'handle': handle,
        'gsrn': gsrn,
        'subject': subject,
        'measurement_id': str(measurement_id),
        'pipeline': 'import_measurements',
        'task': 'poll_batch_status',
    }

    class InvalidBatch(Exception):
        pass

    # Get batch status from ledger
    try:
        response = ledger.get_batch_status(handle)
    except ols.LedgerConnectionError as e:
        logger.exception('Failed to poll ledger for batch status', extra=__log_extra)
        raise task.retry(exc=e)

    # Assert status
    if response.status == ols.BatchStatus.COMMITTED:
        logger.info('Ledger batch status: COMMITTED', extra=__log_extra)
    elif response.status == ols.BatchStatus.INVALID:
        logger.error('Ledger batch status: INVALID', extra=__log_extra)
        raise InvalidBatch('Invalid batch')
    elif response.status == ols.BatchStatus.UNKNOWN:
        logger.info('Ledger batch status: UNKNOWN', extra=__log_extra)
        raise task.retry()
    elif response.status == ols.BatchStatus.PENDING:
        logger.info('Ledger batch status: PENDING', extra=__log_extra)
        raise task.retry()
    else:
        raise RuntimeError('Unknown batch status returned, should NOT have happened!')


@celery_app.task(
    bind=True,
    name='import_measurements.update_measurement_status',
    queue='import-measurements',
    default_retry_delay=POLL_RETRY_DELAY,
    max_retries=POLL_MAX_RETRIES,
)
@logger.wrap_task(
    title='Update published status for Measurement',
    pipeline='import_measurements',
    task='update_measurement_status',
)
def update_measurement_status(task, subject, gsrn, measurement_id):
    """
    Updates the Measurement's published property. The param "published"
    is passed on by the previous task (poll_batch_status).

    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param int measurement_id:
    """
    __log_extra = {
        'gsrn': gsrn,
        'subject': subject,
        'measurement_id': str(measurement_id),
        'pipeline': 'import_measurements',
        'task': 'update_measurement_status',
    }

    @atomic
    def __update_published_status(session):
        """
        Updates published status as an atomic operation
        """
        measurement = MeasurementQuery(session) \
            .has_id(measurement_id) \
            .one()

        measurement.published = True

    try:
        __update_published_status()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Measurement from database', extra=__log_extra)
        raise task.retry(exc=e)


@celery_app.task(
    bind=True,
    name='import_measurements.invoke_webhook',
    queue='import-measurements',
    default_retry_delay=WEBHOOK_RETRY_DELAY,
    max_retries=WEBHOOK_MAX_RETRIES,
)
@logger.wrap_task(
    title='Invoking webhooks on_ggo_issued for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='invoke_webhook',
)
@inject_session
def invoke_webhook(task, subject, gsrn, measurement_id, subscription_id, session):
    """
    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param int measurement_id:
    :param int subscription_id:
    :param sqlalchemy.orm.Session session:
    """
    __log_extra = {
        'subject': subject,
        'measurement_id': str(measurement_id),
        'gsrn': str(gsrn),
        'subscription_id': str(subscription_id),
        'pipeline': 'compose',
        'task': 'invoke_webhook',
    }

    # Get Measurement from database
    try:
        measurement = MeasurementQuery(session) \
            .has_id(measurement_id) \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Measurement from database', extra=__log_extra)
        raise task.retry(exc=e)

    # This should NEVER happen... Anyway:
    if measurement.ggo is None:
        raise RuntimeError('GGO does not exist')

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
        webhook.on_ggo_issued(subscription, measurement.ggo)
    except WebhookConnectionError as e:
        logger.exception('Failed to invoke webhook: ON_GGO_ISSUED (Connection error)', extra=__log_extra)
        raise task.retry(exc=e)
    except WebhookError as e:
        logger.exception('Failed to invoke webhook: ON_GGO_ISSUED', extra=__log_extra)
        raise task.retry(exc=e)
