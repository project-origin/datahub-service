import pytest
import testing.postgresql
import marshmallow_dataclass as md

from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datahub.db import ModelBase
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
)

meteringpoint2 = MeteringPoint(
    sub='SUB2',
    gsrn=gsrn2,
    type=MeasurementType.CONSUMPTION,
    sector='DK1',
)

existing_measurement = Measurement(
    gsrn=gsrn1,
    begin=begin,
    end=begin + timedelta(hours=1),
    amount=100,
    published=True,
)


def seed_meteringpoints_and_measurements(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(existing_measurement)
    session.flush()
    session.commit()


@pytest.fixture(scope='module')
def session():
    """
    Returns a Session object with Ggo + User data seeded for testing
    """
    with testing.postgresql.Postgresql() as psql:
        engine = create_engine(psql.url())
        ModelBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        session = Session()

        seed_meteringpoints_and_measurements(session)

        yield session

        session.close()


# -- Unit tests --------------------------------------------------------------


def test__MeasurementImportController__get_begin__measurements_already_exists__should_continues_after_last_measurement(session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint1, session)

    # Assert
    assert begin == existing_measurement.end


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc))
def test__MeasurementImportController__get_begin__FIRST_MEASUREMENT_TIME_is_defined__should_begin_at_FIRST_MEASUREMENT_TIME(session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint2, session)

    # Assert
    assert begin == datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc)


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=None)
def test__MeasurementImportController__get_begin__NO_measurements_exists_and_FIRST_MEASUREMENT_TIME_is_NOT_defined__should_begin_at_default(session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin(meteringpoint2, session)

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
def test__MeasurementImportController__measurement_exists(gsrn, begin, exists, session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    result = uut.measurement_exists(Mock(gsrn=gsrn, begin=begin), session)

    # Assert
    assert result == exists


# -- Integration tests --------------------------------------------------------


@patch('datahub.measurements.importing.FIRST_MEASUREMENT_TIME', new=datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc))
@patch('datahub.measurements.importing.LAST_MEASUREMENT_TIME', new=datetime(2019, 2, 1, 0, 0, tzinfo=timezone.utc))
@patch('datahub.measurements.importing.eloverblik_service')
def test__MeasurementImportController__integration(eloverblik_service, session):

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

    uut = MeasurementImportController()

    # Act
    uut.import_measurements_for(meteringpoint1, session)
    uut.import_measurements_for(meteringpoint2, session)

    session.commit()

    # Assert
    assert MeasurementQuery(session).has_gsrn(gsrn1).count() == 624
    assert MeasurementQuery(session).has_gsrn(gsrn1).get_first_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 5, 23, 0, tzinfo=timezone.utc)
    assert MeasurementQuery(session).has_gsrn(gsrn1).get_last_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 31, 22, 0, tzinfo=timezone.utc)

    assert MeasurementQuery(session).has_gsrn(gsrn2).count() == 743
    assert MeasurementQuery(session).has_gsrn(gsrn2).get_first_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert MeasurementQuery(session).has_gsrn(gsrn2).get_last_measured_begin().astimezone(timezone.utc) == datetime(2019, 1, 31, 22, 0, tzinfo=timezone.utc)
