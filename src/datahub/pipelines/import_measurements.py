"""
TODO write this
"""
import origin_ledger_sdk as ols
from celery import group, chain
from datetime import datetime, timezone

from datahub import logger
from datahub.ggo import Ggo
from datahub.db import atomic, inject_session
from datahub.common import DateTimeRange
from datahub.settings import LEDGER_URL, GGO_EXPIRE_TIME
from datahub.tasks import celery_app, Retry
from datahub.webhooks import WebhookService
from datahub.meteringpoints import MeteringPointQuery
from datahub.services.eloverblik import EloverblikService
from datahub.measurements import MeasurementQuery, MeasurementImportController


# Settings
POLLING_DELAY = 5
MAX_POLLING_RETRIES = int(3600 / POLLING_DELAY)
SUBMIT_RETRY_DELAY = 30


# Services
service = EloverblikService()
importer = MeasurementImportController()
webhook = WebhookService()


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

    if measurements:
        begin_from = min(m.begin for m in measurements)
        begin_to = max(m.begin for m in measurements)

        if meteringpoint.is_producer():
            # For production measurements, also issue GGOs and invoke webhook
            tasks = chain(
                issue_ggos.s(
                    subject=subject,
                    gsrn=gsrn,
                    begin_from=begin_from.isoformat(),
                    begin_to=begin_to.isoformat(),
                ),
                submit_to_ledger.si(
                    subject=subject,
                    gsrn=gsrn,
                    begin_from=begin_from.isoformat(),
                    begin_to=begin_to.isoformat(),
                ),
                poll_batch_status.s(
                    subject=subject,
                ),
                invoke_webhook.si(
                    subject=meteringpoint.sub,
                    gsrn=gsrn,
                    begin_from=begin_from.isoformat(),
                    begin_to=begin_to.isoformat(),
                ),
            )
        else:
            # For consumption measurements, just submit to ledger without GGOs
            tasks = chain(
                submit_to_ledger.si(
                    subject=subject,
                    gsrn=gsrn,
                    begin_from=begin_from.isoformat(),
                    begin_to=begin_to.isoformat(),
                ),
                poll_batch_status.s(
                    subject=subject,
                ),
            )

        tasks.apply_async()


@celery_app.task(
    name='import_measurements.issue_ggos',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Issuing GGOs for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='issue_ggos',
)
@atomic
def issue_ggos(subject, gsrn, begin_from, begin_to, session):
    """
    :param str subject:
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    :param Session session:
    """
    begin_from = datetime.fromisoformat(begin_from)
    begin_to = datetime.fromisoformat(begin_to)

    query = MeasurementQuery(session) \
        .has_gsrn(gsrn) \
        .begins_within(DateTimeRange(begin=begin_from, end=begin_to)) \
        .needs_ggo_issued()

    logger.info(f'Issuing GGOs for {query.count()} measurements for GSRN: {gsrn}', extra={
        'gsrn': gsrn,
        'subject': subject,
        'begin_from': str(begin_from),
        'begin_to': str(begin_to),
        'pipeline': 'import_measurements',
        'task': 'issue_ggos',
    })

    for i, measurement in enumerate(query.all()):
        session.add(Ggo(
            issue_time=datetime.now(tz=timezone.utc),
            expire_time=datetime.now(tz=timezone.utc) + GGO_EXPIRE_TIME,
            measurement=measurement,
        ))

        if i % 250 == 0:
            session.flush()


@celery_app.task(
    bind=True,
    name='import_measurements.submit_to_ledger',
    autoretry_for=(ols.LedgerException,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Submitting Batch to ledger for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='submit_to_ledger',
)
@inject_session
def submit_to_ledger(task, subject, gsrn, begin_from, begin_to, session):
    """
    :param celery.Task task:
    :param str subject:
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    :param Session session:
    :rtype: str
    """
    begin_from = datetime.fromisoformat(begin_from)
    begin_to = datetime.fromisoformat(begin_to)

    meteringpoint = MeteringPointQuery(session) \
        .has_gsrn(gsrn) \
        .one()

    measurements = MeasurementQuery(session) \
        .has_gsrn(gsrn) \
        .begins_within(DateTimeRange(begin=begin_from, end=begin_to)) \
        .all()

    ledger = ols.Ledger(LEDGER_URL)
    batch = ols.Batch(meteringpoint.key.PrivateKey())

    # Add requests to Batch (for each Measurement + Ggo)
    for measurement in measurements:
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
                max_retries=9999,
                countdown=SUBMIT_RETRY_DELAY,
            )
        else:
            logger.exception(f'Ledger raise an exception for GSRN: {gsrn}', extra={
                'gsrn': gsrn,
                'subject': subject,
                'begin_from': str(begin_from),
                'begin_to': str(begin_to),
                'error_message': str(e),
                'error_code': e.code,
                'pipeline': 'import_measurements',
                'task': 'submit_to_ledger',
            })
            raise

    logger.info(f'Batch submitted to ledger for GSRN: {gsrn}', extra={
        'gsrn': gsrn,
        'subject': subject,
        'begin_from': str(begin_from),
        'begin_to': str(begin_to),
        'handle': handle,
        'pipeline': 'import_measurements',
        'task': 'submit_to_ledger',
    })

    return handle


@celery_app.task(
    bind=True,
    name='import_measurements.poll_batch_status',
    autoretry_for=(ols.LedgerException,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    pipeline='import_measurements',
    task='poll_batch_status',
)
def poll_batch_status(task, handle, subject):
    """
    :param celery.Task task:
    :param str handle:
    :param str subject:
    """
    ledger = ols.Ledger(LEDGER_URL)
    response = ledger.get_batch_status(handle)

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
    name='import_measurements.invoke_webhook',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Invoking webhooks on_ggo_issued for GSRN: %(gsrn)s',
    pipeline='import_measurements',
    task='invoke_webhook',
)
def invoke_webhook(subject, gsrn, begin_from, begin_to):
    """
    :param str subject:
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    """
    webhook.on_ggo_issued(
        subject=subject,
        gsrn=gsrn,
        begin_from=datetime.fromisoformat(begin_from),
        begin_to=datetime.fromisoformat(begin_to),
    )
