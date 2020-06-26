import time
import pytest
import origin_ledger_sdk as ols
from itertools import cycle
from unittest.mock import patch, DEFAULT
from datetime import datetime, timezone, timedelta
from origin_ledger_sdk.ledger_connector import BatchStatusResponse

from datahub.ggo import Ggo
from datahub.measurements import Measurement, MeasurementQuery
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.pipelines.import_measurements import (
    start_import_measurements_pipeline
)
from datahub.webhooks import (
    WebhookSubscription,
    WebhookEvent,
    WebhookConnectionError,
    WebhookError,
)


sub1 = 'SUB1'
sub2 = 'SUB2'

gsrn1 = 'GSRN1'
gsrn2 = 'GSRN2'
gsrn3 = 'GSRN3'
gsrn4 = 'GSRN4'


meteringpoint1 = MeteringPoint(
    sub=sub1,
    gsrn=gsrn1,
    type=MeasurementType.PRODUCTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
    ledger_extended_key=(
        'xprv9s21ZrQH143K2CK5syo8PdeX5Y4TYFkcU'
        'KonHhm1e7znhaKj6odQFbbBa7T2Y77AtiNmU6'
        'aatP2qJBTwvhqxvaSBHA9hEfZ5gViAS3bBj7F'
    ),
)

meteringpoint2 = MeteringPoint(
    sub=sub1,
    gsrn=gsrn2,
    type=MeasurementType.CONSUMPTION,
    sector='DK2',
    technology_code='T010101',
    fuel_code='F01010101',
    ledger_extended_key=(
        'xprv9s21ZrQH143K4WQcTeFMi8gfrgHYuoFH2'
        '63xo4YPAqMN6RGc2BJeAghBtcxf1BzQz81ynY'
        'fZpchrt3tGRBpQn1jp1bNH41AisDWfKQi57MM'
    ),
)

meteringpoint3 = MeteringPoint(
    sub=sub2,
    gsrn=gsrn3,
    type=MeasurementType.PRODUCTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
    ledger_extended_key=(
        'xprv9s21ZrQH143K2SJ98GWKgbEemXLA6SShS'
        'iNTuCAPAeM9RfdYqpqxLxp4ogPSvYfv6tfdSJ'
        'dQo1WTPMatwovVBuWgyBi1RewZC7JUFY9y5Ww'
    ),
)

meteringpoint4 = MeteringPoint(
    sub=sub2,
    gsrn=gsrn4,
    type=MeasurementType.CONSUMPTION,
    sector='DK2',
    technology_code='T010101',
    fuel_code='F01010101',
    ledger_extended_key=(
        'xprv9s21ZrQH143K3twCsVmArteJpkTDmFyz8'
        'p74RhZW27GpTQcAsKUsdTfE17oRLKdHWfRKwm'
        'sPERLVFBL7ucPYthR7TNuN11yQyfPgkU3wfC6'
    ),
)


subscription2 = WebhookSubscription(
    id=1,
    event=WebhookEvent.ON_MEASUREMENT_PUBLISHED,
    subject=sub1,
    url='http://something-else.com',
    secret='something',
)

subscription1 = WebhookSubscription(
    id=2,
    event=WebhookEvent.ON_GGOS_ISSUED,
    subject=sub1,
    url='http://something.com',
    secret='something',
)

subscription4 = WebhookSubscription(
    id=3,
    event=WebhookEvent.ON_MEASUREMENT_PUBLISHED,
    subject=sub2,
    url='http://something-else.com',
    secret='something',
)

subscription3 = WebhookSubscription(
    id=4,
    event=WebhookEvent.ON_GGOS_ISSUED,
    subject=sub2,
    url='http://something.com',
    secret='something',
)


@pytest.fixture(scope='module')
def seeded_session(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(meteringpoint3)
    session.add(meteringpoint4)
    session.add(subscription1)
    session.add(subscription2)
    session.add(subscription3)
    session.add(subscription4)

    # Seed measurements
    # These are "imported" from ElOverblik by the mock importer (ie. inserted
    # into the database during a job, and returned by the importer)
    begins = (
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    for m in (meteringpoint1, meteringpoint2, meteringpoint3, meteringpoint4):
        for begin in begins:
            measurement = Measurement(
                gsrn=m.gsrn,
                begin=begin,
                end=begin + timedelta(hours=1),
                amount=100,
                published=False,
            )

            session.add(measurement)

            if m.type is MeasurementType.PRODUCTION:
                session.add(Ggo(
                    measurement=measurement,
                    issue_time=begin,
                    expire_time=begin,
                ))

    session.flush()
    session.commit()

    yield session


# -- Test cases --------------------------------------------------------------


@patch('datahub.db.make_session')
@patch('datahub.measurements.models.Measurement.build_batch')
@patch('datahub.pipelines.import_measurements.ledger')
@patch('datahub.pipelines.import_measurements.importer')
@patch('datahub.pipelines.import_measurements.webhook_service.on_measurement_published')
@patch('datahub.pipelines.import_measurements.webhook_service.on_ggo_issued')
@patch('datahub.pipelines.import_measurements.get_distinct_gsrn.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.import_measurements.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.submit_to_ledger.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.poll_batch_status.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.update_measurement_status.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.invoke_on_measurement_published_webhook.default_retry_delay', 0)
@patch('datahub.pipelines.import_measurements.invoke_on_ggo_issued_webhook.default_retry_delay', 0)
@pytest.mark.usefixtures('celery_worker')
def test__measurement_published__happy_path__should_publish_measurements_and_issue_ggos_and_invoke_webhooks(
        on_ggo_issued_mock, on_measurement_published_mock, importer_mock,
        ledger_mock, build_batch_mock, make_session_mock, seeded_session):

    make_session_mock.return_value = seeded_session
    build_batch_mock.return_value = 'LEDGER BATCH'

    # -- Arrange -------------------------------------------------------------

    def __import_measurements_for(meteringpoint, session):
        return session.query(Measurement).filter_by(gsrn=meteringpoint.gsrn).all()

    importer_mock.import_measurements_for.side_effect = __import_measurements_for

    on_ggo_issued_mock.side_effect = cycle((
        WebhookConnectionError(),
        WebhookError('', 0, ''),
        DEFAULT,
    ))

    on_measurement_published_mock.side_effect = cycle((
        WebhookConnectionError(),
        WebhookError('', 0, ''),
        DEFAULT,
    ))

    # Executing batch: Raises Ledger exceptions a few times, then returns Handle
    ledger_mock.execute_batch.side_effect = cycle((
        ols.LedgerConnectionError(),
        ols.LedgerException('', code=15),
        ols.LedgerException('', code=17),
        ols.LedgerException('', code=18),
        ols.LedgerException('', code=31),
        'LEDGER-HANDLE',
    ))

    # Getting batch status: Raises LedgerConnectionError once, then returns BatchStatuses
    ledger_mock.get_batch_status.side_effect = cycle((
        ols.LedgerConnectionError(),
        BatchStatusResponse(id='', status=ols.BatchStatus.UNKNOWN),
        BatchStatusResponse(id='', status=ols.BatchStatus.PENDING),
        BatchStatusResponse(id='', status=ols.BatchStatus.COMMITTED),
    ))

    # -- Act -----------------------------------------------------------------

    start_import_measurements_pipeline()

    # -- Assert --------------------------------------------------------------

    # # Wait for pipeline + linked tasks to finish
    time.sleep(10)

    # -- Measurements

    assert all(m.published is True for m in MeasurementQuery(seeded_session))

    # -- ledger.execute_batch()

    assert ledger_mock.execute_batch.call_count == 8 * 6
    assert all(args == (('LEDGER BATCH',),) for args in ledger_mock.execute_batch.call_args_list)

    # -- ledger.get_batch_status()

    assert ledger_mock.get_batch_status.call_count == 8 * 4
    assert all(args == (('LEDGER-HANDLE',),) for args in ledger_mock.get_batch_status.call_args_list)

    # -- webhook_service.on_measurement_published()

    assert on_measurement_published_mock.call_count == 8 * 3

    assert any([
        args for args in on_measurement_published_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_MEASUREMENT_PUBLISHED
        and args[0][0].subject == sub1
        and isinstance(args[0][1], Measurement)
        and args[0][1].sub == sub1
        and args[0][1].gsrn == gsrn1
    ])

    assert any([
        args for args in on_measurement_published_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_MEASUREMENT_PUBLISHED
        and args[0][0].subject == sub1
        and isinstance(args[0][1], Measurement)
        and args[0][1].sub == sub1
        and args[0][1].gsrn == gsrn2
    ])

    assert any([
        args for args in on_measurement_published_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_MEASUREMENT_PUBLISHED
        and args[0][0].subject == sub2
        and isinstance(args[0][1], Measurement)
        and args[0][1].sub == sub2
        and args[0][1].gsrn == gsrn3
    ])

    assert any([
        args for args in on_measurement_published_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_MEASUREMENT_PUBLISHED
        and args[0][0].subject == sub2
        and isinstance(args[0][1], Measurement)
        and args[0][1].sub == sub2
        and args[0][1].gsrn == gsrn4
    ])

    # -- webhook_service.on_ggo_issued()

    assert on_ggo_issued_mock.call_count == 4 * 3

    assert any([
        args for args in on_ggo_issued_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_GGOS_ISSUED
        and args[0][0].subject == sub1
        and isinstance(args[0][1], Ggo)
        and MeasurementQuery(seeded_session).has_id(args[0][1].measurement_id).one().sub == sub1
        and MeasurementQuery(seeded_session).has_id(args[0][1].measurement_id).one().gsrn == gsrn1
    ])

    assert any([
        args for args in on_ggo_issued_mock.call_args_list
        if isinstance(args[0][0], WebhookSubscription)
        and args[0][0].event == WebhookEvent.ON_GGOS_ISSUED
        and args[0][0].subject == sub2
        and isinstance(args[0][1], Ggo)
        and MeasurementQuery(seeded_session).has_id(args[0][1].measurement_id).one().sub == sub2
        and MeasurementQuery(seeded_session).has_id(args[0][1].measurement_id).one().gsrn == gsrn3
    ])
