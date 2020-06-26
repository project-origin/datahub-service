import time
from itertools import cycle

import pytest
from unittest.mock import patch, DEFAULT

from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.services.energytypes.service import EnergyTypeUnavailable
from datahub.pipelines.import_meteringpoints import (
    start_import_meteringpoints_pipeline
)
from datahub.webhooks import (
    WebhookSubscription,
    WebhookEvent,
    WebhookConnectionError,
    WebhookError,
)


TASK_TIMEOUT = 30


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


subscription1 = WebhookSubscription(
    id=1,
    event=WebhookEvent.ON_METERINGPOINTS_AVAILABLE,
    subject=sub1,
    url='http://something-else.com',
    secret='something',
)

subscription2 = WebhookSubscription(
    id=2,
    event=WebhookEvent.ON_METERINGPOINTS_AVAILABLE,
    subject=sub2,
    url='http://something.com',
    secret='something',
)


@pytest.fixture(scope='module')
def seeded_session(session):
    session.add(subscription1)
    session.add(subscription2)
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(meteringpoint3)
    session.add(meteringpoint4)
    session.flush()
    session.commit()

    yield session


# -- Test cases --------------------------------------------------------------


@patch('datahub.db.make_session')
@patch('datahub.pipelines.import_meteringpoints.importer')
@patch('datahub.pipelines.import_meteringpoints.webhook_service.on_meteringpoints_available')
@patch('datahub.pipelines.import_meteringpoints.import_meteringpoints.default_retry_delay', 0)
@patch('datahub.pipelines.import_meteringpoints.invoke_webhook.default_retry_delay', 0)
@pytest.mark.usefixtures('celery_worker')
def test__import_meteringpoints__happy_path__should_invoke_webhooks(
        on_meteringpoints_available_mock, importer_mock, make_session_mock, seeded_session):

    make_session_mock.return_value = seeded_session

    # -- Arrange -------------------------------------------------------------

    def __import_meteringpoints(subject, session):
        return session.query(MeteringPoint).filter_by(sub=subject).all()

    importer_mock.import_meteringpoints.side_effect = __import_meteringpoints

    on_meteringpoints_available_mock.side_effect = cycle((
        WebhookConnectionError(),
        WebhookError('', 0, ''),
        DEFAULT,
    ))

    # -- Act -----------------------------------------------------------------

    start_import_meteringpoints_pipeline(sub1, seeded_session)

    # -- Assert --------------------------------------------------------------

    # # Wait for pipeline + linked tasks to finish
    time.sleep(10)

    # webhook_service.on_meteringpoints_available()

    assert on_meteringpoints_available_mock.call_count == 3
    assert on_meteringpoints_available_mock.call_args[0][0].id == subscription1.id


@patch('datahub.db.make_session')
@patch('datahub.pipelines.import_meteringpoints.importer')
@patch('datahub.pipelines.import_meteringpoints.webhook_service.on_meteringpoints_available')
@patch('datahub.pipelines.import_meteringpoints.import_meteringpoints.default_retry_delay', 0)
@patch('datahub.pipelines.import_meteringpoints.invoke_webhook.default_retry_delay', 0)
@pytest.mark.usefixtures('celery_worker')
def test__import_meteringpoints__importer_raises_EnergyTypeUnavailable__should_NOT_invoke_webhooks(
        on_meteringpoints_available_mock, importer_mock, make_session_mock, seeded_session):

    make_session_mock.return_value = seeded_session
    importer_mock.import_meteringpoints.side_effect = EnergyTypeUnavailable

    # -- Act -----------------------------------------------------------------

    start_import_meteringpoints_pipeline(sub1, seeded_session)
    start_import_meteringpoints_pipeline(sub2, seeded_session)

    # -- Assert --------------------------------------------------------------

    time.sleep(10)

    on_meteringpoints_available_mock.assert_not_called()
