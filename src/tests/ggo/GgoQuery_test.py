import pytest
import testing.postgresql
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
from itertools import product

from datahub.common import DateTimeRange
from datahub.db import ModelBase
from datahub.ggo import Ggo, GgoQuery
from datahub.measurements import Measurement
from datahub.meteringpoints import MeteringPoint, MeasurementType


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


def seed_ggo_test_data(session):

    # Dependencies
    session.add(meteringpoint1)
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(meteringpoint3)
    session.add(meteringpoint4)
    session.flush()
    session.commit()

    # Input for combinations
    meteringpoints = (
        meteringpoint1,
        meteringpoint2,
        meteringpoint3,
        meteringpoint4,
    )
    begin = (
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    # Combinations
    combinations = product(meteringpoints, begin)

    # Seed GGOs
    for i, (m, b) in enumerate(combinations, start=1):
        measurement = Measurement(
            gsrn=m.gsrn,
            begin=b,
            end=b + timedelta(hours=1),
            amount=100,
            published=(i % 2 == 0),
        )

        session.add(measurement)

        if m.type is MeasurementType.PRODUCTION:
            session.add(Ggo(
                issue_time=datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                expire_time=datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                measurement=measurement,
            ))

        if i % 500 == 0:
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
        seed_ggo_test_data(session1)
        session1.close()

        session2 = Session()
        yield session2
        session2.close()


# -- TEST CASES --------------------------------------------------------------


@pytest.mark.parametrize('sub', (sub1, sub2))
def test__GgoQuery__belongs_to__returns_correct_ggos(seeded_session, sub):
    query = GgoQuery(seeded_session) \
        .belongs_to(sub)

    assert query.count() > 0
    assert all(ggo.measurement.sub == sub for ggo in query.all())


@pytest.mark.parametrize('ggo_begin', (
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
))
def test__GgoQuery__begins_at__returns_correct_ggos(seeded_session, ggo_begin):
    query = GgoQuery(seeded_session) \
        .begins_at(ggo_begin)

    assert query.count() > 0
    assert all(ggo.begin == ggo_begin for ggo in query.all())


@pytest.mark.parametrize('ggo_begin', (
        datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2031, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
))
def test__GgoQuery__begins_at__Ggo_does_not_exist__returs_nothing(seeded_session, ggo_begin):
    query = GgoQuery(seeded_session) \
        .begins_at(ggo_begin)

    assert query.count() == 0


@pytest.mark.parametrize('begin, end', (
        (
                datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
        (
                datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
        (
                datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2030, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
))
def test__GgoQuery__begins_within__returns_correct_ggos(seeded_session, begin, end):
    query = GgoQuery(seeded_session) \
        .begins_within(DateTimeRange(begin=begin, end=end))

    assert query.count() > 0
    assert all(begin <= ggo.begin <= end for ggo in query.all())


@pytest.mark.parametrize('begin, end', (
        (
                datetime(2010, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2019, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
        ),
        (
                datetime(2021, 2, 1, 2, 0, 0, tzinfo=timezone.utc),
                datetime(2030, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
))
def test__GgoQuery__begins_within__Ggo_does_not_exist__returs_nothing(seeded_session, begin, end):
    query = GgoQuery(seeded_session) \
        .begins_within(DateTimeRange(begin=begin, end=end))

    assert query.count() == 0


@pytest.mark.parametrize('gsrn', (gsrn1, gsrn3))
def test__GgoQuery__has_gsrn__Ggo_exists__returns_correct_ggos(seeded_session, gsrn):
    query = GgoQuery(seeded_session) \
        .has_gsrn(gsrn)

    assert query.count() > 0
    assert all(ggo.gsrn == gsrn for ggo in query.all())


@pytest.mark.parametrize('gsrn', (gsrn2, gsrn4))
def test__GgoQuery__has_gsrn__Ggo_does_not_exist__returs_nothing(seeded_session, gsrn):
    query = GgoQuery(seeded_session) \
        .has_gsrn(gsrn)

    assert query.count() == 0
    assert query.one_or_none() is None


@pytest.mark.parametrize('published', (True, False))
def test__GgoQuery__is_published__returns_correct_ggos(seeded_session, published):
    query = GgoQuery(seeded_session) \
        .is_published(published)

    assert query.count() > 0
    assert all(ggo.measurement.published == published for ggo in query.all())
