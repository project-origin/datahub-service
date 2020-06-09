import pytest
import testing.postgresql
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datahub.db import ModelBase
from datahub.measurements import Measurement, MeasurementImportController
from datahub.meteringpoints import MeteringPoint, MeasurementType


# -- Test data ---------------------------------------------------------------

gsrn1 = 'GSRN1'
gsrn2 = 'GSRN2'
begin = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)


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
    submitted=True,
    gsrn=gsrn1,
    begin=begin,
    end=begin + timedelta(hours=1),
    amount=100,
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


# -- Constructor -------------------------------------------------------------


@patch('origin.ggo.composer.datahub_service')
@patch('origin.ggo.issuing.datahub_service')
def test__MeasurementImportController__get_begin__measurements_already_exists__TODO(session):

    # Arrange
    uut = MeasurementImportController()

    # Act
    begin = uut.get_begin()

