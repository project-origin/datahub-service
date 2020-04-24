"""
TODO write this
"""
import logging
import origin_ledger_sdk as ols
from celery import group, chain
from datetime import datetime, timezone

from datahub.ggo import Ggo
from datahub.db import atomic, inject_session
from datahub.common import DateTimeRange
from datahub.settings import LEDGER_URL, GGO_EXPIRE_TIME
from datahub.tasks import celery_app
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
    get_distinct_gsrn.s() \
        .apply_async()


def start_import_measurements_pipeline_for(gsrn):
    """
    TODO

    :param str gsrn:
    """
    import_measurements.s(gsrn) \
        .apply_async()


@celery_app.task(name='import_measurements.get_distinct_gsrn')
@inject_session
def get_distinct_gsrn(session):
    """
    :param Session session:
    """
    logging.info('--- get_distinct_gsrn')

    meteringpoints = MeteringPointQuery(session).all()

    tasks = [
        import_measurements.s(meteringpoint.gsrn)
        for meteringpoint in meteringpoints
    ]

    if tasks:
        group(tasks).apply_async()


@celery_app.task(name='import_measurements.import_measurements')
@inject_session
def import_measurements(gsrn, session):
    """
    :param str gsrn:
    :param Session session:
    """
    logging.info(f'--- import_measurements, gsrn={gsrn}')

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
                issue_ggos.s(gsrn, begin_from.isoformat(), begin_to.isoformat()),
                submit_to_ledger.si(gsrn, begin_from.isoformat(), begin_to.isoformat()),
                poll_batch_status.s(),
                invoke_webhook.si(meteringpoint.sub, gsrn, begin_from.isoformat(), begin_to.isoformat()),
            )
        else:
            # For consumption measurements, just submit to ledger without GGOs
            tasks = chain(
                submit_to_ledger.si(gsrn, begin_from.isoformat(), begin_to.isoformat()),
                poll_batch_status.s(),
            )

        tasks.apply_async()


@celery_app.task(name='import_measurements.issue_ggos')
@atomic
def issue_ggos(gsrn, begin_from, begin_to, session):
    """
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    :param Session session:
    """
    logging.info((
        f'--- issue_ggos, gsrn={gsrn}, '
        f'begin_from={begin_from}, begin_to={begin_to}'
    ))

    begin_from = datetime.fromisoformat(begin_from)
    begin_to = datetime.fromisoformat(begin_to)

    query = MeasurementQuery(session) \
        .has_gsrn(gsrn) \
        .begins_within(DateTimeRange(begin=begin_from, end=begin_to)) \
        .needs_ggo_issued()

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
    max_retries=None,
)
@inject_session
def submit_to_ledger(task, gsrn, begin_from, begin_to, session):
    """
    :param celery.Task task:
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    :param Session session:
    :rtype: str
    """
    logging.info((
        f'--- submit_to_ledger, gsrn={gsrn}, '
        f'begin_from={begin_from}, begin_to={begin_to}'
    ))

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
            # Ledger queue is full, try again later
            logging.info('ERROR 31, RETRYING...')
            raise task.retry(countdown=SUBMIT_RETRY_DELAY)
        else:
            raise e

    return handle


@celery_app.task(
    bind=True,
    name='import_measurements.poll_batch_status',
    max_retries=MAX_POLLING_RETRIES,
)
def poll_batch_status(task, handle):
    """
    :param celery.Task task:
    :param str handle:
    """
    logging.info('--- poll_batch_status, handle = %s' % handle)

    ledger = ols.Ledger(LEDGER_URL)
    response = ledger.get_batch_status(handle)

    if response.status == ols.BatchStatus.COMMITTED:
        pass
    elif response.status == ols.BatchStatus.INVALID:
        raise Exception('INVALID')
    else:
        raise task.retry(countdown=POLLING_DELAY)


@celery_app.task(name='import_measurements.invoke_webhook')
def invoke_webhook(sub, gsrn, begin_from, begin_to):
    """
    :param str sub:
    :param str gsrn:
    :param str begin_from:
    :param str begin_to:
    """
    logging.info('--- invoke_webhook, gsrn=%s, begin_from=%s, begin_to=%s' % (
        gsrn, begin_from, begin_to))

    webhook.on_ggo_issued(
        subject=sub,
        gsrn=gsrn,
        begin_from=datetime.fromisoformat(begin_from),
        begin_to=datetime.fromisoformat(begin_to),
    )
