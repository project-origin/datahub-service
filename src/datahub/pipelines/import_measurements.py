"""
TODO write this
"""
import origin_ledger_sdk as ols
from celery import group, chain

from datahub import logger
from datahub.db import atomic, inject_session
from datahub.settings import LEDGER_URL, DEBUG
from datahub.tasks import celery_app
from datahub.webhooks import WebhookService
from datahub.meteringpoints import MeteringPointQuery, MeasurementType
from datahub.services.eloverblik import EloverblikService
from datahub.measurements import MeasurementQuery, MeasurementImportController


# Settings
POLLING_DELAY = 5
MAX_POLLING_RETRIES = int(3600 / POLLING_DELAY)
SUBMIT_RETRY_COUNT = 9999
SUBMIT_RETRY_DELAY = 30


# Services
service = EloverblikService()
importer = MeasurementImportController()
webhook = WebhookService()
ledger = ols.Ledger(LEDGER_URL, verify=not DEBUG)


def start_import_measurements_pipeline():
    """
    TODO
    """
    get_distinct_gsrn \
        .s() \
        .apply_async()


def start_import_measurements_pipeline_for(subject, gsrn):
    """
    TODO

    :param str subject:
    :param str gsrn:
    """
    import_measurements \
        .s(subject=subject, gsrn=gsrn) \
        .apply_async()


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
    max_retries=5,
)
@logger.wrap_task(
    title='Importing measurements for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='import_measurements',
)
@inject_session
def import_measurements(subject, gsrn, session):
    """
    :param str subject:
    :param str gsrn:
    :param Session session:
    """
    meteringpoint = MeteringPointQuery(session) \
        .has_gsrn(gsrn) \
        .one()

    measurements = importer.import_measurements_for(meteringpoint)

    for measurement in measurements:
        tasks = [
            # Submit Batch with Measurement (and Ggo if PRODUCTION)
            submit_to_ledger.si(
                subject=subject,
                measurement_id=measurement.id,
            ),

            # Poll for Batch status
            poll_batch_status.s(
                subject=subject,
            ),

            # Update Measurement.published status attribute
            update_measurement_status.si(
                subject=subject,
                measurement_id=measurement.id,
            ),
        ]

        # If PRODUCTION, also invoke OnGgoIssued webhook
        if meteringpoint.type is MeasurementType.PRODUCTION:
            tasks.append(invoke_webhook.si(
                subject=meteringpoint.sub,
                gsrn=gsrn,
                measurement_id=measurement.id,
            ))

        chain(*tasks).apply_async()


@celery_app.task(
    bind=True,
    name='import_measurements.submit_to_ledger',
    queue='import-measurements',
    autoretry_for=(ols.LedgerException,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Submitting Batch to ledger for Measurement: %(measurement_id)d',
    pipeline='import_measurements',
    task='submit_to_ledger',
)
@inject_session
def submit_to_ledger(task, subject, measurement_id, session):
    """
    :param celery.Task task:
    :param str subject:
    :param int measurement_id:
    :param Session session:
    :rtype: str
    """
    measurement = MeasurementQuery(session) \
        .has_id(measurement_id) \
        .one()

    # Build ledger Batch
    batch = ols.Batch(measurement.meteringpoint.key.PrivateKey())
    batch.add_request(measurement.get_ledger_publishing_request())
    if measurement.ggo:
        batch.add_request(measurement.ggo.get_ledger_issuing_request())

    # Submit batch
    try:
        handle = ledger.execute_batch(batch)
    except ols.LedgerException as e:
        if e.code == 31:
            # Ledger Queue is full
            raise task.retry(
                max_retries=SUBMIT_RETRY_COUNT,
                countdown=SUBMIT_RETRY_DELAY,
            )
        else:
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

    return handle


@celery_app.task(
    bind=True,
    name='import_measurements.poll_batch_status',
    queue='import-measurements',
    autoretry_for=(ols.LedgerException,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Poll batch status',
    pipeline='import_measurements',
    task='poll_batch_status',
)
def poll_batch_status(task, handle, subject):
    """
    :param celery.Task task:
    :param str handle:
    :param str subject:
    """
    try:
        response = ledger.get_batch_status(handle)
    except ols.LedgerException as e:
        logger.exception(f'Ledger raise an exception when polling handle', extra={
            'handle': handle,
            'subject': subject,
            'error_message': str(e),
            'error_code': e.code,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
        raise

    if response.status == ols.BatchStatus.COMMITTED:
        logger.error('Ledger submitted', extra={
            'subject': subject,
            'handle': handle,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
    elif response.status == ols.BatchStatus.INVALID:
        logger.error('Batch submit FAILED: Invalid', extra={
            'subject': subject,
            'handle': handle,
            'pipeline': 'import_measurements',
            'task': 'submit_to_ledger',
        })
    else:
        raise task.retry(
            max_retries=MAX_POLLING_RETRIES,
            countdown=POLLING_DELAY,
        )


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
def update_measurement_status(subject, measurement_id, session):
    """
    :param str subject:
    :param int measurement_id:
    :param Session session:
    """
    measurement = MeasurementQuery(session) \
        .has_id(measurement_id) \
        .one()

    measurement.published = True


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
