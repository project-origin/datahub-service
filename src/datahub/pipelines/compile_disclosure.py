"""
TODO write this
"""
import origin_ledger_sdk as ols
from celery import chord, group

from datahub import logger
from datahub.db import atomic, inject_session
from datahub.disclosure import Disclosure, DisclosureState, DisclosureSettlement, DisclosureRetiredGgo
from datahub.measurements import Measurement
from datahub.settings import LEDGER_URL, DEBUG
from datahub.tasks import celery_app
from datahub.webhooks import WebhookService
from datahub.services.eloverblik import EloverblikService
from datahub.meteringpoints import MeteringPointsImportController


service = EloverblikService()
importer = MeteringPointsImportController()
webhook = WebhookService()


def start_compile_disclosure_pipeline(disclosure):
    """
    TODO

    :param Disclosure disclosure:
    """
    get_measurements \
        .s(subject=disclosure.sub, disclosure_id=disclosure.id) \
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


@celery_app.task(
    name='compile_disclosure.get_measurements',
    queue='disclosure',
    # autoretry_for=(Exception,),
    # retry_backoff=2,
    # max_retries=5,
)
@logger.wrap_task(
    title='Starting to compile Disclosure',
    pipeline='compile_disclosure',
    task='get_measurements',
)
@atomic
def get_measurements(subject, disclosure_id, session):
    """
    :param str subject:
    :param int disclosure_id:
    :param Session session:
    """
    disclosure = session.query(Disclosure) \
        .filter(Disclosure.id == disclosure_id) \
        .one_or_none()

    if disclosure is None:
        logger.error('Failed to compile: Disclosure not found', extra={
            'subject': subject,
            'disclosure_id': disclosure_id,
            'pipeline': 'disclosure',
            'task': 'get_measurements',
        })
        return

    tasks = []

    for measurement in disclosure.get_measurements():
        tasks.append(get_settlement_and_ggos_from_ledger.si(
            subject=subject,
            disclosure_id=disclosure_id,
            measurement_id=measurement.id,
        ))

    # Update Disclosure state
    disclosure.state = DisclosureState.PROCESSING

    # Start tasks with on-complete callback (update_disclosure_state)
    chord(tasks)(update_disclosure_state.si(
        subject=subject,
        disclosure_id=disclosure_id,
    ))


@celery_app.task(
    name='compile_disclosure.get_settlement_and_ggos_from_ledger',
    queue='disclosure',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Importing settlement and GGOs from ledger',
    pipeline='compile_disclosure',
    task='get_settlement_and_ggos_from_ledger',
)
@atomic
def get_settlement_and_ggos_from_ledger(subject, disclosure_id, measurement_id, session):
    """
    :param str subject:
    :param int disclosure_id:
    :param int measurement_id:
    :param Session session:
    """
    # Get disclosure
    disclosure = session.query(Disclosure) \
        .filter(Disclosure.id == disclosure_id) \
        .one_or_none()

    if disclosure is None:
        logger.error('Failed to compile: Disclosure not found', extra={
            'subject': subject,
            'disclosure_id': disclosure_id,
            'measurement_id': measurement_id,
            'pipeline': 'disclosure',
            'task': 'get_settlement_and_ggos_from_ledger',
        })
        return

    # Get measurement
    measurement = session.query(Measurement) \
        .filter(Measurement.id == measurement_id) \
        .one_or_none()

    if measurement is None:
        logger.error('Failed to compile: Measurement not found', extra={
            'subject': subject,
            'disclosure_id': disclosure_id,
            'measurement_id': measurement_id,
            'pipeline': 'disclosure',
            'task': 'get_settlement_and_ggos_from_ledger',
        })
        return

    # Get settlement from ledger
    ledger = ols.Ledger(LEDGER_URL, verify=not DEBUG)

    try:
        ledger_settlement = ledger.get_settlement(measurement.settlement_address)
    except ols.LedgerException as e:
        if e.code == 75:
            # No settlement exists (nothing at the requested address)
            return
        else:
            # Arbitrary ledger exception, let it bubble up
            raise

    # Get settlement from database
    settlement = session.query(DisclosureSettlement) \
        .filter(DisclosureSettlement.disclosure_id == disclosure.id) \
        .filter(DisclosureSettlement.address == measurement.settlement_address) \
        .one_or_none()

    # Create new settlement if not existing in database
    if settlement is None:
        settlement = DisclosureSettlement(
            disclosure=disclosure,
            measurement=measurement,
            address=ledger_settlement.address,
        )
        session.add(settlement)

    for part in ledger_settlement.parts:
        count = session.query(DisclosureRetiredGgo) \
            .filter(DisclosureRetiredGgo.settlement_id == settlement.id) \
            .filter(DisclosureRetiredGgo.address == part.ggo) \
            .count()

        if count == 0:
            ledger_ggo = ledger.get_ggo(part.ggo)

            session.add(DisclosureRetiredGgo(
                settlement=settlement,
                address=ledger_ggo.address,
                amount=ledger_ggo.amount,
                begin=ledger_ggo.begin,
                end=ledger_ggo.end,
                sector=ledger_ggo.sector,
                technology_code=ledger_ggo.tech_type,
                fuel_code=ledger_ggo.fuel_type,
            ))


@celery_app.task(
    name='compile_disclosure.update_disclosure_state',
    queue='disclosure',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Importing settlement and GGOs from ledger',
    pipeline='compile_disclosure',
    task='update_disclosure_state',
)
@atomic
def update_disclosure_state(subject, disclosure_id, session):
    """
    :param str subject:
    :param int disclosure_id:
    :param Session session:
    """

    # Get disclosure
    disclosure = session.query(Disclosure) \
        .filter(Disclosure.id == disclosure_id) \
        .one_or_none()

    if disclosure is None:
        logger.error('Failed to compile: Disclosure not found', extra={
            'subject': subject,
            'disclosure_id': disclosure_id,
            'pipeline': 'disclosure',
            'task': 'get_settlement_and_ggos_from_ledger',
        })
        return

    disclosure.state = DisclosureState.AVAILABLE
