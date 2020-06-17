import pytest
import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datahub.db import ModelBase
from datahub.meteringpoints import (
    MeteringPoint,
    MeasurementType,
    MeteringPointQuery,
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
)

meteringpoint2 = MeteringPoint(
    sub=sub1,
    gsrn=gsrn2,
    type=MeasurementType.CONSUMPTION,
    sector='DK2',
    technology_code='T010101',
    fuel_code='F01010101',
)

meteringpoint3 = MeteringPoint(
    sub=sub2,
    gsrn=gsrn3,
    type=MeasurementType.PRODUCTION,
    sector='DK1',
    technology_code='T010101',
    fuel_code='F01010101',
)

meteringpoint4 = MeteringPoint(
    sub=sub2,
    gsrn=gsrn4,
    type=MeasurementType.CONSUMPTION,
    sector='DK2',
    technology_code='T010101',
    fuel_code='F01010101',
)


def seed_test_data(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(meteringpoint3)
    session.add(meteringpoint4)
    session.flush()
    session.commit()


@pytest.fixture(scope='module')
def seeded_session():
    """
    Returns a Session object with Ggo + User data seeded for testing
    """
    with testing.postgresql.Postgresql() as psql:
        engine = create_engine(psql.url())
        ModelBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, expire_on_commit=False)

        session1 = Session()
        seed_test_data(session1)
        session1.close()

        session2 = Session()
        yield session2
        session2.close()


# -- TEST CASES --------------------------------------------------------------


@pytest.mark.parametrize('sub', (sub1, sub2))
def test__MeteringPointQuery__belongs_to__MeteringPoint_exists__rreturns_correct_meteringpoints(seeded_session, sub):
    query = MeteringPointQuery(seeded_session) \
        .belongs_to(sub)

    assert query.count() > 0
    assert all(m.sub == sub for m in query.all())


def test__MeteringPointQuery__belongs_to__MeteringPoint_does_not_exist__returs_nothing(seeded_session):
    query = MeteringPointQuery(seeded_session) \
        .belongs_to('a subject that does not exists')

    assert query.count() == 0


@pytest.mark.parametrize('gsrn', (gsrn1, gsrn2, gsrn3, gsrn4))
def test__MeteringPointQuery__has_gsrn__MeteringPoint_exists__returns_correct_MeteringPoint(seeded_session, gsrn):
    query = MeteringPointQuery(seeded_session) \
        .has_gsrn(gsrn)

    assert query.count() == 1
    assert query.one().gsrn == gsrn


def test__MeteringPointQuery__has_gsrn__MeteringPoint_does_not_exist__returs_nothing(seeded_session):
    query = MeteringPointQuery(seeded_session) \
        .has_gsrn('a GSRN not present in the database')

    assert query.count() == 0
    assert query.one_or_none() is None


@pytest.mark.parametrize('type', (
    MeasurementType.PRODUCTION,
    MeasurementType.CONSUMPTION,
))
def test__MeteringPointQuery__is_type__MeteringPoints_exists__returns_correct_meteringpoints(seeded_session, type):
    query = MeteringPointQuery(seeded_session) \
        .is_type(type)

    assert query.count() > 0
    assert all(m.type == type for m in query.all())


def test__MeteringPointQuery__is_production__MeteringPoints_exists__returns_correct_meteringpoints(seeded_session):
    query = MeteringPointQuery(seeded_session) \
        .is_production()

    assert query.count() > 0
    assert all(m.type == MeasurementType.PRODUCTION for m in query.all())


def test__MeteringPointQuery__is_consumption__MeteringPoints_exists__returns_correct_meteringpoints(seeded_session):
    query = MeteringPointQuery(seeded_session) \
        .is_consumption()

    assert query.count() > 0
    assert all(m.type == MeasurementType.CONSUMPTION for m in query.all())
