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

from datahub import logger
from datahub.db import atomic, inject_session
from datahub.tasks import celery_app
from datahub.webhooks import WebhookService
from datahub.meteringpoints import MeteringPointQuery, MeasurementType, MeteringPoint
from datahub.services.eloverblik import EloverblikService
from datahub.settings import LEDGER_URL, DEBUG, BATCH_RESUBMIT_AFTER_HOURS
from datahub.measurements import (
    Measurement,
    MeasurementQuery,
    MeasurementImportController,
)


# Settings
RETRY_MAX_DELAY = 60
MAX_RETRIES = (BATCH_RESUBMIT_AFTER_HOURS * 60 * 60) / RETRY_MAX_DELAY


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


def start_submit_measurement_pipeline(measurement, meteringpoint):
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
    """
    build_submit_measurement_pipeline(measurement, meteringpoint) \
        .apply_async()


def build_submit_measurement_pipeline(measurement, meteringpoint):
    """
    Builds and returns TODO
    """
    tasks = [
        # Submit Batch with Measurement (and Ggo if PRODUCTION)
        submit_to_ledger.si(
            subject=meteringpoint.sub,
            measurement_id=measurement.id,
        ),

        # Poll for Batch status
        poll_batch_status.s(
            subject=meteringpoint.sub,
            measurement_id=measurement.id,
        ),

        # Update Measurement.published status attribute
        update_measurement_status.s(
            subject=meteringpoint.sub,
            measurement_id=measurement.id,
        ),
    ]

    # If PRODUCTION, also invoke OnGgoIssued webhook
    if meteringpoint.type is MeasurementType.PRODUCTION:
        tasks.append(invoke_webhook.si(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
            measurement_id=measurement.id,
        ))

    return chain(*tasks)


@celery_app.task(
    name='import_measurements.get_distinct_gsrn',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Getting distinct GSRN numbers for importing measurements',
    pipeline='import_measurements',
    task='get_distinct_gsrn',
)
@inject_session
def get_distinct_gsrn(session):
    """
    Fetches all distinct GSRN numbers from the database, and starts a
    import_measurements() pipelines for each of them.

    :param Session session:
    """
    meteringpoints = MeteringPointQuery(session).all()

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
    name='import_measurements.import_measurements',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    retry_backoff_max=RETRY_MAX_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Importing measurements for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='import_measurements',
)
@inject_session
def import_measurements(subject, gsrn, session):
    """
    Imports measurements for a single MeteringPoint, and starts a
    start_submit_measurement_pipeline() pipeline for each of the newly
    imported measurements.

    :param str subject:
    :param str gsrn:
    :param Session session:
    """

    meteringpoint = MeteringPointQuery(session) \
        .has_gsrn(gsrn) \
        .one()

    @atomic
    def __import_measurements(session):
        return importer.import_measurements_for(meteringpoint, session)

    for measurement in __import_measurements():
        start_submit_measurement_pipeline(measurement, meteringpoint)


@celery_app.task(
    name='import_measurements.submit_to_ledger',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    retry_backoff_max=RETRY_MAX_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Submitting Batch to ledger for Measurement: %(measurement_id)d',
    pipeline='import_measurements',
    task='submit_to_ledger',
)
@atomic
def submit_to_ledger(subject, measurement_id, session):
    """
    Submits a single measurement (and its associated GGO, if any) to the
    ledger. Returns the ledger Handle, which is passed on to the next chained
    task (poll_batch_status).

    :param str subject:
    :param int measurement_id:
    :param Session session:
    :rtype: str
    """
    measurement = MeasurementQuery(session) \
        .has_id(measurement_id) \
        .one()

    # Build ledger Batch
    # TODO Move this to Measurement.build_batch() ?
    batch = ols.Batch(measurement.meteringpoint.key.PrivateKey())
    batch.add_request(measurement.get_ledger_publishing_request())
    if measurement.ggo:
        batch.add_request(measurement.ggo.get_ledger_issuing_request())

    # Submit batch
    try:
        with logger.tracer.span('ExecuteBatch'):
            handle = ledger.execute_batch(batch)
    except ols.LedgerException as e:
        # (e.code == 31) means Ledger Queue is full
        # In this case, don't log the error, just try again later
        if e.code != 31:
            logger.exception(f'Ledger raise an exception for GSRN: {measurement.meteringpoint.gsrn}', extra={
                'gsrn': measurement.meteringpoint.gsrn,
                'subject': subject,
                'measurement_id': str(measurement_id),
                'error_message': str(e),
                'error_code': e.code,
                'pipeline': 'import_measurements',
                'task': 'submit_to_ledger',
            })

        raise

    logger.info(f'Batch submitted to ledger for GSRN: {measurement.meteringpoint.gsrn}', extra={
        'gsrn': measurement.meteringpoint.gsrn,
        'subject': subject,
        'measurement_id': str(measurement_id),
        'handle': handle,
        'pipeline': 'import_measurements',
        'task': 'submit_to_ledger',
    })

    measurement.set_submitted_to_ledger()

    return handle


@celery_app.task(
    name='import_measurements.poll_batch_status',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    retry_backoff_max=RETRY_MAX_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Poll batch status',
    pipeline='import_measurements',
    task='poll_batch_status',
)
def poll_batch_status(handle, subject, measurement_id):
    """
    Polls the ledger for Batch status until it has been completed/declined.
    The param "handle" is passed on by the previous task (submit_to_ledger).

    :param str handle:
    :param str subject:
    :param int measurement_id:
    :rtype: bool
    """
    with logger.tracer.span('GetBatchStatus'):
        response = ledger.get_batch_status(handle)

    if response.status == ols.BatchStatus.COMMITTED:
        logger.error('Ledger submitted', extra={
            'subject': subject,
            'handle': handle,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
        return True
    elif response.status == ols.BatchStatus.INVALID:
        logger.error('Batch submit FAILED: Invalid', extra={
            'subject': subject,
            'handle': handle,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
        return False
    elif response.status == ols.BatchStatus.UNKNOWN:
        logger.error('Batch submit UNKNOWN: Retrying', extra={
            'subject': subject,
            'handle': handle,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
        raise Exception('Retry task')
    else:
        raise Exception('Retry task')


@celery_app.task(
    name='import_measurements.update_measurement_status',
    queue='import-measurements',
    autoretry_for=(ols.LedgerException,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Update status for Measurement: %(measurement_id)d',
    pipeline='import_measurements',
    task='update_measurement_status',
)
@atomic
def update_measurement_status(published, subject, measurement_id, session):
    """
    Updates the Measurement's published property. The param "published"
    is passed on by the previous task (poll_batch_status).

    :param bool published:
    :param str subject:
    :param int measurement_id:
    :param Session session:
    """
    measurement = MeasurementQuery(session) \
        .has_id(measurement_id) \
        .one()

    measurement.published = measurement.published or published


@celery_app.task(
    name='import_measurements.invoke_webhook',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Invoking webhooks on_ggo_issued for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='invoke_webhook',
)
@inject_session
def invoke_webhook(subject, gsrn, measurement_id, session):
    """
    invokes the "GGO ISSUED" webhook.

    :param str subject:
    :param str gsrn:
    :param int measurement_id:
    :param Session session:
    """
    measurement = MeasurementQuery(session) \
        .has_id(measurement_id) \
        .one()

    webhook.on_ggo_issued(
        subject=subject,
        gsrn=gsrn,
        begin=measurement.begin,
    )
