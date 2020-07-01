"""
TODO write this
"""
import origin_ledger_sdk as ols
from sqlalchemy import orm
from celery import chord, group, shared_task

from datahub import logger
from datahub.db import atomic, inject_session
from datahub.measurements import Measurement
from datahub.tasks import celery_app
from datahub.disclosure import Disclosure, DisclosureState, DisclosureCompiler


# Settings
RETRY_DELAY = 15
MAX_RETRIES = (12 * 60 * 60) / RETRY_DELAY


# Services
compiler = DisclosureCompiler()


def start_compile_disclosure_pipeline(disclosure):
    """
    TODO

    :param Disclosure disclosure:
    """
    get_measurements \
        .s(subject=disclosure.sub, disclosure_id=disclosure.id) \
        .apply_async()


def start_compile_all_disclosures_pipeline():
    """
    TODO
    """
    get_disclosures \
        .s() \
        .apply_async()


@celery_app.task(
    name='compile_disclosure.get_disclosures',
    queue='disclosure',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Getting all disclosures from database',
    pipeline='compile_disclosure',
    task='get_disclosures',
)
@inject_session
def get_disclosures(session):
    """
    :param Session session:
    """
    tasks = []

    for disclosure in session.query(Disclosure).all():
        tasks.append(get_measurements.si(
            subject=disclosure.sub,
            disclosure_id=disclosure.id,
        ))

    group(*tasks).apply_async()


@shared_task(
    bind=True,
    name='compile_disclosure.get_measurements',
    queue='disclosure',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Starting to compile Disclosure',
    pipeline='compile_disclosure',
    task='get_measurements',
)
def get_measurements(task, subject, disclosure_id):
    """
    :param celery.Task task:
    :param str subject:
    :param int disclosure_id:
    """
    __log_extra = {
        'subject': subject,
        'disclosure_id': disclosure_id,
        'pipeline': 'compile_disclosure',
        'task': 'get_measurements',
    }

    @atomic
    def __get_measurements_and_update_disclosure_state(session):
        disclosure = session.query(Disclosure) \
            .filter(Disclosure.id == disclosure_id) \
            .one()

        disclosure.state = DisclosureState.PROCESSING

        return disclosure.get_measurements()

    try:
        measurements = __get_measurements_and_update_disclosure_state()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Disclosure Measurements from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    tasks = [
        get_settlement_and_ggos_from_ledger.si(
            subject=subject,
            disclosure_id=disclosure_id,
            measurement_id=measurement.id,
        )
        for measurement in measurements
    ]

    # Start tasks with on-complete callback (update_disclosure_state)
    chord(tasks)(update_disclosure_state.si(
        subject=subject,
        disclosure_id=disclosure_id,
    ))


@celery_app.task(
    bind=True,
    name='compile_disclosure.get_settlement_and_ggos_from_ledger',
    queue='disclosure',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Importing settlement and GGOs from ledger',
    pipeline='compile_disclosure',
    task='get_settlement_and_ggos_from_ledger',
)
@atomic
def get_settlement_and_ggos_from_ledger(task, subject, disclosure_id, measurement_id, session):
    """
    :param celery.Task task:
    :param str subject:
    :param int disclosure_id:
    :param int measurement_id:
    :param Session session:
    """
    __log_extra = {
        'subject': subject,
        'disclosure_id': disclosure_id,
        'measurement_id': measurement_id,
        'pipeline': 'compile_disclosure',
        'task': 'get_settlement_and_ggos_from_ledger',
    }

    # Get Disclosure from DB
    try:
        disclosure = session.query(Disclosure) \
            .filter(Disclosure.id == disclosure_id) \
            .one()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Disclosure from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Get Measurement from DB
    try:
        measurement = session.query(Measurement) \
            .filter(Measurement.id == measurement_id) \
            .one_or_none()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to load Measurement from database, retrying...', extra=__log_extra)
        raise task.retry(exc=e)

    # Synchronize disclosure for this measurement
    try:
        # TODO what if this fails otherwise, for instance DB connection errors?
        compiler.sync_for_measurement(disclosure, measurement, session)
    except ols.LedgerConnectionError as e:
        logger.exception('Failed to connect to ledger, retrying...', extra=__log_extra)
        raise task.retry(exc=e)


@celery_app.task(
    bind=True,
    name='compile_disclosure.update_disclosure_state',
    queue='disclosure',
    default_retry_delay=RETRY_DELAY,
    max_retries=MAX_RETRIES,
)
@logger.wrap_task(
    title='Importing settlement and GGOs from ledger',
    pipeline='compile_disclosure',
    task='update_disclosure_state',
)
def update_disclosure_state(task, subject, disclosure_id):
    """
    :param celery.Task task:
    :param str subject:
    :param int disclosure_id:
    """
    __log_extra = {
        'subject': subject,
        'disclosure_id': disclosure_id,
        'pipeline': 'compile_disclosure',
        'task': 'update_disclosure_state',
    }

    @atomic
    def __update_state(session):
        disclosure = session.query(Disclosure) \
            .filter(Disclosure.id == disclosure_id) \
            .one()

        disclosure.state = DisclosureState.AVAILABLE

    # Get Disclosure from DB
    try:
        __update_state()
    except orm.exc.NoResultFound:
        raise
    except Exception as e:
        logger.exception('Failed to update Disclosure state to AVAILABLE, retrying...', extra=__log_extra)
        raise task.retry(exc=e)
