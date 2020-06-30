import pytest
import origin_ledger_sdk as ols
from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta, date

from datahub.ggo import Ggo
from datahub.common import SummaryResolution
from datahub.measurements import Measurement
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.disclosure import (
    Disclosure,
    DisclosureState,
    DisclosureCompiler,
    DisclosureMeteringPoint,
    DisclosureSettlement,
    DisclosureRetiredGgo,
)


# -- Test data ---------------------------------------------------------------


meteringpoint1 = MeteringPoint(
    sub='SUB1',
    gsrn='gsrn1',
    type=MeasurementType.CONSUMPTION,
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
    sub='SUB2',
    gsrn='gsrn2',
    type=MeasurementType.CONSUMPTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
    ledger_extended_key=(
        'xprv9s21ZrQH143K2CK5syo8PdeX5Y4TYFkcU'
        'KonHhm1e7znhaKj6odQFbbBa7T2Y77AtiNmU6'
        'aatP2qJBTwvhqxvaSBHA9hEfZ5gViAS3bBj7F'
    ),
)

disclosure1 = Disclosure(
    state=DisclosureState.AVAILABLE,
    sub='sub',
    begin=date(2020, 1, 1),
    end=date(2020, 2, 1),
    name='',
    max_resolution=SummaryResolution.all,
    publicize_meteringpoints=True,
    publicize_gsrn=True,
    publicize_physical_address=True,
)

disclosure2 = Disclosure(
    state=DisclosureState.AVAILABLE,
    sub='sub',
    begin=date(2020, 1, 1),
    end=date(2020, 2, 1),
    name='',
    max_resolution=SummaryResolution.all,
    publicize_meteringpoints=True,
    publicize_gsrn=True,
    publicize_physical_address=True,
)

disclosure3 = Disclosure(
    state=DisclosureState.AVAILABLE,
    sub='sub',
    begin=date(2020, 1, 1),
    end=date(2020, 2, 1),
    name='',
    max_resolution=SummaryResolution.all,
    publicize_meteringpoints=True,
    publicize_gsrn=True,
    publicize_physical_address=True,
)

measurement1 = Measurement(
    gsrn='gsrn1',
    begin=datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
    end=datetime(2020, 1, 1, 1, 0, tzinfo=timezone.utc),
    amount=100,
    published=True,
)

measurement2 = Measurement(
    gsrn='gsrn1',
    begin=datetime(2020, 1, 1, 1, 0, tzinfo=timezone.utc),
    end=datetime(2020, 1, 1, 2, 0, tzinfo=timezone.utc),
    amount=100,
    published=True,
)

measurement3 = Measurement(
    gsrn='gsrn1',
    begin=datetime(2020, 1, 1, 2, 0, tzinfo=timezone.utc),
    end=datetime(2020, 1, 1, 3, 0, tzinfo=timezone.utc),
    amount=400,
    published=True,
)


@pytest.fixture(scope='module')
def seeded_session(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(disclosure1)
    session.add(disclosure2)
    session.add(disclosure3)
    session.add(measurement1)
    session.add(measurement2)
    session.add(measurement3)
    session.flush()

    settlement = DisclosureSettlement(
        disclosure=disclosure1,
        measurement=measurement1,
        address=measurement1.settlement_address,
    )

    session.add(settlement)
    session.flush()

    session.add(DisclosureMeteringPoint(
        disclosure=disclosure1,
        meteringpoint=meteringpoint1,
    ))
    session.add(DisclosureMeteringPoint(
        disclosure=disclosure1,
        meteringpoint=meteringpoint2,
    ))
    session.add(DisclosureRetiredGgo(
        settlement=settlement,
        address='retired-ggo-address',
        amount=100,
        begin=date(2020, 1, 1),
        end=date(2020, 2, 1),
        sector='DK1',
        technology_code='',
        fuel_code='',
    ))

    session.flush()
    session.commit()

    yield session


# -- get_ledger_settlement() -------------------------------------------------


@patch('datahub.disclosure.compiler.ledger')
def test__DisclosureCompiler__get_ledger_settlement__ledger_raises_error_75__should_return_None(ledger_mock):

    # Arrange
    ledger_mock.get_settlement.side_effect = ols.LedgerException('', 75)
    measurement = Mock(settlement_address='ASDASD')
    uut = DisclosureCompiler()

    # Act
    ledger_settlement = uut.get_ledger_settlement(measurement)

    # Assert
    assert ledger_settlement is None
    ledger_mock.get_settlement.assert_called_once_with(measurement.settlement_address)


@patch('datahub.disclosure.compiler.ledger')
@pytest.mark.parametrize('exc', (
    ols.LedgerException('', 1),
    Exception('foobar'),
))
def test__DisclosureCompiler__get_ledger_settlement__ledger_raises_arbitrary_exception__should_reraise(ledger_mock, exc):

    # Arrange
    ledger_mock.get_settlement.side_effect = exc
    measurement = Mock(settlement_address='ASDASD')
    uut = DisclosureCompiler()

    # Act
    with pytest.raises(type(exc)):
        uut.get_ledger_settlement(measurement)


@patch('datahub.disclosure.compiler.ledger')
def test__DisclosureCompiler__ledget_invoked_correctly__should_return_ledger_settlement(ledger_mock):

    # Arrange
    settlement = Mock(settlement_address='ASDASD')
    measurement = Mock(settlement_address='ASDASD')
    ledger_mock.get_settlement.return_value = settlement
    uut = DisclosureCompiler()

    # Act
    ledger_settlement = uut.get_ledger_settlement(measurement)

    # Assert
    assert ledger_settlement is settlement
    ledger_mock.get_settlement.assert_called_once_with(measurement.settlement_address)


# -- get_or_create_settlement() ----------------------------------------------


def test__DisclosureCompiler__get_or_create_settlement__does_exist__should_return_correct_settlement(seeded_session):

    # Arrange
    uut = DisclosureCompiler()

    # Act
    disclosure_settlement = uut.get_or_create_settlement(disclosure1, measurement1, seeded_session)

    # Assert
    assert disclosure_settlement.id == 1
    assert disclosure_settlement.disclosure_id == disclosure1.id
    assert disclosure_settlement.measurement_id == measurement1.id


def test__DisclosureCompiler__get_or_create_settlement__disclosure_does_exists__should_create_new_settlement(seeded_session):

    # Arrange
    uut = DisclosureCompiler()

    # Act
    disclosure_settlement = uut.get_or_create_settlement(disclosure2, measurement1, seeded_session)

    # Assert
    assert disclosure_settlement.id != 1
    assert disclosure_settlement.disclosure_id == disclosure2.id
    assert disclosure_settlement.measurement_id == measurement1.id


def test__DisclosureCompiler__get_or_create_settlement__measurement_does_exists__should_create_new_settlement(seeded_session):

    # Arrange
    uut = DisclosureCompiler()

    # Act
    disclosure_settlement = uut.get_or_create_settlement(disclosure1, measurement2, seeded_session)

    # Assert
    assert disclosure_settlement.id != 1
    assert disclosure_settlement.disclosure_id == disclosure1.id
    assert disclosure_settlement.measurement_id == measurement2.id


# -- get_ledger_settlement() -------------------------------------------------


def test__DisclosureCompiler__disclosure_retired_ggo_exists__disclosure_settlement_does_NOT_exist__should_return_False(seeded_session):

    # Arrange
    disclosure_settlement = Mock(id=9999)
    settlement_part = Mock(ggo='retired-ggo-address')
    uut = DisclosureCompiler()

    # Act
    result = uut.disclosure_retired_ggo_exists(
        disclosure_settlement, settlement_part, seeded_session)

    # Assert
    assert result is False


def test__DisclosureCompiler__disclosure_retired_ggo_exists__settlement_part_does_NOT_exist__should_return_False(seeded_session):

    # Arrange
    disclosure_settlement = seeded_session.query(DisclosureSettlement).filter_by(id=1).one()
    settlement_part = Mock(ggo='retired-ggo-address-does-not-exist')
    uut = DisclosureCompiler()

    # Act
    result = uut.disclosure_retired_ggo_exists(
        disclosure_settlement, settlement_part, seeded_session)

    # Assert
    assert result is False


def test__DisclosureCompiler__disclosure_retired_ggo_exists__does_exist__should_return_True(seeded_session):

    # Arrange
    disclosure_settlement = seeded_session.query(DisclosureSettlement).filter_by(id=1).one()
    settlement_part = Mock(ggo='retired-ggo-address')
    uut = DisclosureCompiler()

    # Act
    result = uut.disclosure_retired_ggo_exists(
        disclosure_settlement, settlement_part, seeded_session)

    # Assert
    assert result is True


# -- Integration test --------------------------------------------------------


@patch('datahub.disclosure.compiler.ledger')
def test__DisclosureCompiler__integration__should_sync_DB_with_ledger(ledger_mock, seeded_session):

    ledger_ggos = {
        'ggo-address-1': ols.GGO(
            origin='origin',
            amount=101,
            begin=measurement3.begin,
            end=measurement3.end,
            sector=measurement3.sector,
            tech_type=measurement3.technology_code,
            fuel_type=measurement3.fuel_code,
            address='ggo-address-1',
        ),
        'ggo-address-2': ols.GGO(
            origin='',
            amount=101,
            begin=measurement3.begin,
            end=measurement3.end,
            sector=measurement3.sector,
            tech_type=measurement3.technology_code,
            fuel_type=measurement3.fuel_code,
            address='ggo-address-2',
        ),
        'ggo-address-3': ols.GGO(
            origin='',
            amount=101,
            begin=measurement3.begin,
            end=measurement3.end,
            sector=measurement3.sector,
            tech_type=measurement3.technology_code,
            fuel_type=measurement3.fuel_code,
            address='ggo-address-3',
        ),
    }

    def __ledger_get_ggo_mock(address):
        return ledger_ggos[address]

    # Arrange
    ledger_mock.get_ggo.side_effect = __ledger_get_ggo_mock
    ledger_mock.get_settlement.side_effect = (
        # First invocation
        ols.ledger_dto.Settlement(
            measurement=measurement3.address,
            address=measurement3.settlement_address,
            parts=[
                ols.ledger_dto.SettlementPart(ggo='ggo-address-1', amount=101),
                ols.ledger_dto.SettlementPart(ggo='ggo-address-2', amount=102),
            ]
        ),
        # Second invocation (one more GGO has been retired
        ols.ledger_dto.Settlement(
            measurement=measurement3.address,
            address=measurement3.settlement_address,
            parts=[
                ols.ledger_dto.SettlementPart(ggo='ggo-address-1', amount=101),
                ols.ledger_dto.SettlementPart(ggo='ggo-address-2', amount=102),
                ols.ledger_dto.SettlementPart(ggo='ggo-address-3', amount=103),
            ]
        ),
    )

    uut = DisclosureCompiler()

    # Act
    uut.sync_for_measurement(disclosure3, measurement3, seeded_session)
    uut.sync_for_measurement(disclosure3, measurement3, seeded_session)
    seeded_session.flush()
    seeded_session.commit()

    # Assert
    settlement = seeded_session \
        .query(DisclosureSettlement) \
        .filter_by(disclosure_id=disclosure3.id) \
        .filter_by(measurement_id=measurement3.id) \
        .one()

    assert len(settlement.ggos) == 3

    for addr in ('ggo-address-1', 'ggo-address-2', 'ggo-address-3'):
        disclosure_ggo = seeded_session \
            .query(DisclosureRetiredGgo) \
            .filter_by(settlement_id=settlement.id) \
            .filter_by(address=addr) \
            .one()

        assert disclosure_ggo.address == ledger_ggos[addr].address
        assert disclosure_ggo.amount == ledger_ggos[addr].amount
        assert disclosure_ggo.begin == ledger_ggos[addr].begin
        assert disclosure_ggo.end == ledger_ggos[addr].end
        assert disclosure_ggo.sector == ledger_ggos[addr].sector
        assert disclosure_ggo.technology_code == ledger_ggos[addr].tech_type
        assert disclosure_ggo.fuel_code == ledger_ggos[addr].fuel_type
