import pytest
import marshmallow_dataclass as md
from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta

from datahub.ggo import Ggo
from datahub.ggo.queries import GgoQuery
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.services.eloverblik import GetTimeSeriesResponse
from datahub.measurements import (
    Measurement,
    MeasurementImportController,
    MeasurementQuery,
)

from .imported_timeseries_data import (
    IMPORTED_TIMESERIES_DATA_GSRN1,
    IMPORTED_TIMESERIES_DATA_GSRN2,
)


# -- Test data ---------------------------------------------------------------

gsrn1 = '269841726498172468'
gsrn2 = '198498465435784984'
begin = datetime(2019, 1, 5, 23, 0, tzinfo=timezone.utc)


meteringpoint1 = MeteringPoint(
    sub='SUB1',
    gsrn=gsrn1,
    type=MeasurementType.PRODUCTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
)

meteringpoint2 = MeteringPoint(
    sub='SUB2',
    gsrn=gsrn2,
    type=MeasurementType.CONSUMPTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
)

existing_measurement = Measurement(
    gsrn=gsrn1,
    begin=begin,
    end=begin + timedelta(hours=1),
    amount=100,
    published=True,
    ggo=Ggo(
        issue_time=datetime.now(),
        expire_time=datetime.now(),
    )
)


@pytest.fixture(scope='module')
def seeded_session(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(existing_measurement)
    session.flush()
    session.commit()

    yield session


# -- Unit tests --------------------------------------------------------------


def test__MeasurementImportController__get_begin__measurements_already_exists__should_continues_after_last_measurement(seeded_session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint1, seeded_session)

    # Assert
    assert begin == existing_measurement.end


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc))
def test__MeasurementImportController__get_begin__FIRST_MEASUREMENT_TIME_is_defined__should_begin_at_FIRST_MEASUREMENT_TIME(seeded_session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint2, seeded_session)

    # Assert
    assert begin == datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc)


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=None)
def test__MeasurementImportController__get_begin__NO_measurements_exists_and_FIRST_MEASUREMENT_TIME_is_NOT_defined__should_begin_at_default(seeded_session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint2, seeded_session)

    # Assert
    assert begin == uut.get_default_begin()


@pytest.mark.parametrize('last_measurement_time, default_end, expected_end', (
        (
                datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
        (
                datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
))
def test__MeasurementImportController__get_end__LAST_MEASUREMENT_TIME_is_defined__should_end_at_LAST_MEASUREMENT_TIME(
        last_measurement_time, default_end, expected_end):
    """
    Should return the LAST_MEASUREMENT_TIME unless its later than today (NOW),
    in which case it should never return date later than today.
    """

    # Arrange
    uut = MeasurementImportController()
    uut.get_default_end = Mock(return_value=default_end)

    # Act
    with patch('datahub.measurements.importing.LAST_MEASUREMENT_TIME', new=last_measurement_time):
        end = uut.get_end()

    # Assert
    assert end == expected_end


@patch('datahub.measurements.importing.LAST_MEASUREMENT_TIME', new=None)
def test__MeasurementImportController__get_end__LAST_MEASUREMENT_TIME_is_NOT_defined__should_end_at_default():

    # Arrange
    uut = MeasurementImportController()

    # Act
    end = uut.get_end()

    # Assert
    assert end == uut.get_default_end()


@pytest.mark.parametrize('gsrn, begin, exists', (
        (gsrn1, begin, True),
        (gsrn2, begin, False),
        (gsrn1, datetime(2050, 1, 1, 0, 0, tzinfo=timezone.utc), False),
        (gsrn2, datetime(2050, 1, 1, 0, 0, tzinfo=timezone.utc), False),
))
def test__MeasurementImportController__measurement_exists(gsrn, begin, exists, seeded_session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    result = uut.measurement_exists(Mock(gsrn=gsrn, begin=begin), seeded_session)

    # Assert
    assert result == exists


# -- Integration tests --------------------------------------------------------


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc))
@patch('datahub.measurements.importing.LAST_MEASUREMENT_TIME', new=datetime(2019, 2, 1, 0, 0, tzinfo=timezone.utc))
@patch('datahub.measurements.importing.eloverblik_service')
@patch('datahub.measurements.importing.energtype_service')
def test__MeasurementImportController__integration(energtype_service, eloverblik_service, seeded_session):

    def __get_time_series(gsrn, date_from, date_to):
        eloverblik_response_schema = md.class_schema(GetTimeSeriesResponse)
        eloverblik_response = eloverblik_response_schema()
        if gsrn == gsrn1:
            return eloverblik_response.loads(IMPORTED_TIMESERIES_DATA_GSRN1).result
        elif gsrn == gsrn2:
            return eloverblik_response.loads(IMPORTED_TIMESERIES_DATA_GSRN2).result
        else:
            raise RuntimeError

    # Arrange
    eloverblik_service.get_time_series.side_effect = __get_time_series
    energtype_service.get_emissions.return_value = {}

    uut = MeasurementImportController()

    # Act
    uut.import_measurements_for(meteringpoint1, seeded_session)
    uut.import_measurements_for(meteringpoint2, seeded_session)

    seeded_session.commit()

    # Assert
    assert MeasurementQuery(seeded_session).has_gsrn(gsrn1).count() == 624
    assert MeasurementQuery(seeded_session).has_gsrn(gsrn1).get_first_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 5, 23, 0, tzinfo=timezone.utc)
    assert MeasurementQuery(seeded_session).has_gsrn(gsrn1).get_last_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 31, 22, 0, tzinfo=timezone.utc)
    assert GgoQuery(seeded_session).has_gsrn(gsrn1).count() == 624
    assert all(m.ggo is not None for m in MeasurementQuery(seeded_session).has_gsrn(gsrn1))

    assert MeasurementQuery(seeded_session).has_gsrn(gsrn2).count() == 743
    assert MeasurementQuery(seeded_session).has_gsrn(gsrn2).get_first_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert MeasurementQuery(seeded_session).has_gsrn(gsrn2).get_last_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 31, 22, 0, tzinfo=timezone.utc)
    assert GgoQuery(seeded_session).has_gsrn(gsrn2).count() == 0
    assert all(m.ggo is None for m in MeasurementQuery(seeded_session).has_gsrn(gsrn2))
