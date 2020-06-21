import pytest
from itertools import product
from datetime import datetime, timedelta, timezone

from datahub.common import DateTimeRange, SummaryResolution
from datahub.measurements.queries import MeasurementSummary
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.measurements import Measurement, MeasurementQuery


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


@pytest.fixture(scope='module')
def seeded_session(session):
    session.add(meteringpoint1)
    session.add(meteringpoint2)
    session.add(meteringpoint3)
    session.add(meteringpoint4)
    session.flush()

    # Input for combinations
    gsrn = (
        meteringpoint1.gsrn,
        meteringpoint2.gsrn,
        meteringpoint3.gsrn,
        meteringpoint4.gsrn,
    )
    begin = (
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    # Combinations
    combinations = product(gsrn, begin)

    # Seed GGOs
    for i, (g, b) in enumerate(combinations, start=1):
        session.add(Measurement(
            gsrn=g,
            begin=b,
            end=b + timedelta(hours=1),
            amount=100,
            published=(i % 2 == 0),
        ))

        if i % 500 == 0:
            session.flush()

    session.commit()
    session.commit()

    yield session


# -- TEST CASES --------------------------------------------------------------


@pytest.mark.parametrize('sub', (sub1, sub2))
def test__MeasurementQuery__belongs_to__returns_correct_measurements(seeded_session, sub):
    query = MeasurementQuery(seeded_session) \
        .belongs_to(sub)

    assert query.count() > 0
    assert all(m.sub == sub for m in query.all())


def test__MeasurementQuery__belongs_to__Measurement_does_not_exist__returs_nothing(seeded_session):
    query = MeasurementQuery(seeded_session) \
        .belongs_to('a subject that does not exists')

    assert query.count() == 0


@pytest.mark.parametrize('begin', (
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
))
def test__MeasurementQuery__begins_at__returns_correct_measurements(seeded_session, begin):
    query = MeasurementQuery(seeded_session) \
        .begins_at(begin)

    assert query.count() > 0
    assert all(m.begin == begin for m in query.all())


@pytest.mark.parametrize('begin', (
        datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2030, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2031, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
))
def test__MeasurementQuery__begins_at__Measurement_does_not_exist__returs_nothing(seeded_session, begin):
    query = MeasurementQuery(seeded_session) \
        .begins_at(begin)

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
def test__MeasurementQuery__begins_within__returns_correct_measurements(seeded_session, begin, end):
    query = MeasurementQuery(seeded_session) \
        .begins_within(DateTimeRange(begin=begin, end=end))

    assert query.count() > 0
    assert all(begin <= m.begin <= end for m in query.all())


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
def test__MeasurementQuery__begins_within__Measurement_does_not_exist__returs_nothing(seeded_session, begin, end):
    query = MeasurementQuery(seeded_session) \
        .begins_within(DateTimeRange(begin=begin, end=end))

    assert query.count() == 0


@pytest.mark.parametrize('measurement_id', (1, 2))
def test__MeasurementQuery__has_id__Measurement_exists__returns_correct_Measurement(seeded_session, measurement_id):
    query = MeasurementQuery(seeded_session) \
        .has_id(measurement_id)

    assert query.count() == 1
    assert query.one().id == measurement_id


@pytest.mark.parametrize('measurement_id', (-1, 0))
def test__MeasurementQuery__has_id__Measurement_does_not_exist__returs_nothing(seeded_session, measurement_id):
    query = MeasurementQuery(seeded_session) \
        .has_id(measurement_id)

    assert query.count() == 0
    assert query.one_or_none() is None


@pytest.mark.parametrize('gsrn', (gsrn1, gsrn2, gsrn3, gsrn4))
def test__MeasurementQuery__has_gsrn__Measurement_exists__returns_correct_Measurements(seeded_session, gsrn):
    query = MeasurementQuery(seeded_session) \
        .has_gsrn(gsrn)

    assert query.count() > 0
    assert all(m.gsrn == gsrn for m in query.all())


def test__MeasurementQuery__has_gsrn__Measurement_does_not_exist__returs_nothing(seeded_session):
    query = MeasurementQuery(seeded_session) \
        .has_gsrn('a GSRN not present in the database')

    assert query.count() == 0
    assert query.one_or_none() is None


@pytest.mark.parametrize('gsrn', (
        [gsrn1, 'a GSRN not present in the database'],
        [gsrn1],
        [gsrn1, gsrn2],
        [gsrn3, gsrn4],
))
def test__MeasurementQuery__has_any_gsrn__Measurement_exists__returns_correct_Measurements(seeded_session, gsrn):
    query = MeasurementQuery(seeded_session) \
        .has_any_gsrn(gsrn)

    assert query.count() > 0
    assert all(m.gsrn in gsrn for m in query.all())


def test__MeasurementQuery__has_any_gsrn__Measurement_does_not_exist__returs_nothing(seeded_session):
    query = MeasurementQuery(seeded_session) \
        .has_any_gsrn(['a GSRN not present in the database', 'another'])

    assert query.count() == 0
    assert query.one_or_none() is None


@pytest.mark.parametrize('type', (
    MeasurementType.PRODUCTION,
    MeasurementType.CONSUMPTION,
))
def test__MeasurementQuery__is_type__Measurement_exists__returns_correct_Measurements(seeded_session, type):
    query = MeasurementQuery(seeded_session) \
        .is_type(type)

    assert query.count() > 0
    assert all(m.type == type for m in query.all())


def test__MeasurementQuery__is_production__Measurement_exists__returns_correct_Measurements(seeded_session):
    query = MeasurementQuery(seeded_session) \
        .is_production()

    assert query.count() > 0
    assert all(m.type == MeasurementType.PRODUCTION for m in query.all())


def test__MeasurementQuery__is_consumption__Measurement_exists__returns_correct_Measurements(seeded_session):
    query = MeasurementQuery(seeded_session) \
        .is_consumption()

    assert query.count() > 0
    assert all(m.type == MeasurementType.CONSUMPTION for m in query.all())


@pytest.mark.parametrize('is_published', (True, False))
def test__MeasurementQuery__is_published__Measurement_exists__returns_correct_Measurements(seeded_session, is_published):
    query = MeasurementQuery(seeded_session) \
        .is_published(is_published)

    assert query.count() > 0
    assert all(m.published == is_published for m in query.all())


# TODO needs_resubmit_to_ledger


def test__MeasurementQuery__get_distinct_begins__Measurement_exists__returns_list_of_begins(seeded_session):
    begins = MeasurementQuery(seeded_session) \
        .get_distinct_begins()

    assert sorted(begins) == sorted((
        datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc),
    ))


def test__MeasurementQuery__get_distinct_begins__Measurement_does_not_exist__returs_empty_list(seeded_session):
    begins = MeasurementQuery(seeded_session) \
        .has_id(-1) \
        .get_distinct_begins()

    assert begins == []


def test__MeasurementQuery__get_first_measured_begin__Measurement_exists__returns_correct_begin(seeded_session):
    begin = MeasurementQuery(seeded_session) \
        .get_first_measured_begin()

    assert begin == datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test__MeasurementQuery__get_first_measured_begin__Measurement_does_not_exist__returns_None(seeded_session):
    begin = MeasurementQuery(seeded_session) \
        .has_id(-1) \
        .get_first_measured_begin()

    assert begin is None


def test__MeasurementQuery__get_last_measured_begin__Measurement_exists__returns_correct_begin(seeded_session):
    begin = MeasurementQuery(seeded_session) \
        .get_last_measured_begin()

    assert begin == datetime(2021, 2, 1, 1, 0, 0, tzinfo=timezone.utc)


def test__MeasurementQuery__get_last_measured_begin__Measurement_does_not_exist__returns_None(seeded_session):
    begin = MeasurementQuery(seeded_session) \
        .has_id(-1) \
        .get_last_measured_begin()

    assert begin is None


def test__MeasurementQuery__get_summary__returns_new_MeasurementSummary(seeded_session):
    summary = MeasurementQuery(seeded_session) \
        .get_summary(SummaryResolution.day, [])

    assert isinstance(summary, MeasurementSummary)
