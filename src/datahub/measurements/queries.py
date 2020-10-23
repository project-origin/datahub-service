import sqlalchemy as sa
from sqlalchemy import func, bindparam, text
from datetime import datetime, timezone
from itertools import groupby
from functools import lru_cache
from sqlalchemy.orm import joinedload

from datahub.ggo import Ggo
from datahub.common import DateTimeRange, LabelRange
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.settings import BATCH_RESUBMIT_AFTER_HOURS

from .models import (
    Measurement,
    MeasurementFilters,
    SummaryResolution,
    SummaryGroup,
)


class MeasurementQuery(object):
    """
    Abstraction around querying Measurement objects from the database,
    supporting cascade calls to combine filters.

    Usage example::

        query = MeasurementQuery(session) \
            .belongs_to('65e58f0b-62dd-40c2-a540-f773b0beed66') \
            .begins_at(datetime(2020, 1, 1, 0, 0))

        for measurement in query:
            pass

    Attributes not present on the GgoQuery class is redirected to
    SQLAlchemy's Query object, like count(), all() etc., for example::

        query = MeasurementQuery(session) \
            .belongs_to('65e58f0b-62dd-40c2-a540-f773b0beed66') \
            .begins_at(datetime(2020, 1, 1, 0, 0)) \
            .offset(100) \
            .limit(20) \
            .count()
    """
    def __init__(self, session, q=None):
        """
        :param sa.orm.Session session:
        :param sa.orm.Query q:
        """
        self.session = session
        if q is not None:
            self.q = q
        else:
            self.q = session.query(Measurement) \
                .join(MeteringPoint, MeteringPoint.gsrn == Measurement.gsrn) \
                .outerjoin(Ggo, Ggo.measurement_id == Measurement.id) \
                .options(joinedload(Measurement.ggo)) \
                .options(joinedload(Measurement.meteringpoint))

    def __iter__(self):
        return iter(self.q)

    def __getattr__(self, name):
        return getattr(self.q, name)

    def apply_filters(self, filters):
        """
        Apply filters using a MeasurementFilters object.

        :param MeasurementFilters filters:
        :rtype: MeasurementQuery
        """
        q = self.q

        if filters.gsrn:
            q = q.filter(Measurement.gsrn.in_(filters.gsrn))
        if filters.begin:
            q = q.filter(Measurement.begin == filters.begin.astimezone(timezone.utc))
        elif filters.begin_range:
            q = q.filter(Measurement.begin >= filters.begin_range.begin.astimezone(timezone.utc))
            q = q.filter(Measurement.begin <= filters.begin_range.end.astimezone(timezone.utc))
        if filters.sector:
            q = q.filter(MeteringPoint.sector.in_(filters.sector))
        if filters.type:
            q = q.filter(MeteringPoint.type == filters.type)

        return self.__class__(self.session, q)

    def is_active(self):
        """
        Only include measurements from meteringpoints which are active.

        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.disabled.is_(False),
        ))

    def belongs_to(self, sub):
        """
        Only include measurements which belong to the user identified by
        the provided sub (subject).

        :param str sub:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def begins_at(self, begin):
        """
        Only include measurements which begins at the provided datetime.

        :param datetime begin:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.begin == begin.astimezone(timezone.utc),
        ))

    def begins_within(self, begin_range):
        """
        Only include measurements which begins within the provided datetime
        range (both begin and end are included).

        :param DateTimeRange begin_range:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(sa.and_(
            Measurement.begin >= begin_range.begin.astimezone(timezone.utc),
            Measurement.begin <= begin_range.end.astimezone(timezone.utc),
        )))

    def has_id(self, id):
        """
        Only include the measurement with a specific ID.

        :param int id:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.id == id,
        ))

    def has_gsrn(self, gsrn):
        """
        Only include measurements which were measured by the MeteringPoint
        identified with the provided GSRN number.

        :param str gsrn:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.gsrn == gsrn,
        ))

    def has_any_gsrn(self, gsrn):
        """
        Only include measurements which were measured by any of the
        MeteringPoints identified with the provided GSRN numbers.

        :param list[str] gsrn:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.gsrn.in_(gsrn),
        ))

    def is_type(self, type):
        """
        Only include measurements of the provided type,
        ie. PRODUCTION or CONSUMPTION.

        :param MeasurementType type:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.type == type,
        ))

    def is_production(self):
        """
        Only include measurements of type PRODUCTION.

        :rtype: MeasurementQuery
        """
        return self.is_type(MeasurementType.PRODUCTION)

    def is_consumption(self):
        """
        Only include measurements of type CONSUMPTION.

        :rtype: MeasurementQuery
        """
        return self.is_type(MeasurementType.CONSUMPTION)

    def is_published(self, value=True):
        """
        Only include measurements that has been published to the ledger,
        ie. are publicly available.

        :param bool value:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.published == value,
        ))

    def needs_resubmit_to_ledger(self):
        """
        Only include measurements that needs to be resubmitted to the ledger.
        These are the measurements that has been imported but,
        for some reason, the ledger transaction has not completed.

        These are defined by a certain period of time after they were
        created, or last submitted to the ledger, without the transaction
        completing.

        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.published.is_(False),
            sa.or_(
                sa.and_(
                    Measurement.submitted.is_(None),
                    Measurement.created <= text(
                        "NOW() - INTERVAL '%d HOURS'" % BATCH_RESUBMIT_AFTER_HOURS),
                ),
                Measurement.submitted <= text(
                    "NOW() - INTERVAL '%d HOURS'" % BATCH_RESUBMIT_AFTER_HOURS),
            ),
        ))

    def get_distinct_begins(self):
        """
        Returns a list of all distinct Measurement.begin in the result set.

        :rtype: list[datetime]
        """
        return [row[0] for row in self.session.query(
            self.q.subquery().c.begin.distinct())]

    def get_first_measured_begin(self):
        """
        Returns the first Measurement.begin in the result set.

        :rtype: datetime
        """
        return self.session.query(
            func.min(self.q.subquery().c.begin)).scalar()

    def get_last_measured_begin(self):
        """
        Returns the last Measurement.begin in the result set.

        :rtype: datetime
        """
        return self.session.query(
            func.max(self.q.subquery().c.begin)).scalar()

    def get_summary(self, resolution, grouping, utc_offset=0):
        """
        Returns a summary of the result set.

        :param SummaryResolution resolution:
        :param list[str] grouping:
        :param int utc_offset:
        :rtype: MeasurementSummary
        """
        return MeasurementSummary(
            self.session, self, resolution, grouping, utc_offset)


class MeasurementSummary(object):
    """
    Implements a summary/aggregation of measurements.

    Provided a MeasurementQuery, this class compiles together a list of
    SummaryGroups, where each group is defined by the "grouping" parameter.
    The aggregated data is based on the result set of the query provided.
    It essentially works by wrapping a SQL "GROUP BY" statement.

    The parameter "resolution" defined the returned data resolution.
    Call .fill() before accessing .labels or .groups to fill gaps in data.
    """

    GROUPINGS = (
        'type',
        'gsrn',
        'sector',
    )

    RESOLUTIONS_POSTGRES = {
        SummaryResolution.hour: 'YYYY-MM-DD HH24:00',
        SummaryResolution.day: 'YYYY-MM-DD',
        SummaryResolution.month: 'YYYY-MM',
        SummaryResolution.year: 'YYYY',
    }

    ALL_TIME_LABEL = 'All-time'

    def __init__(self, session, query, resolution, grouping, utc_offset=0):
        """
        :param sa.orm.Session session:
        :param MeasurementQuery query:
        :param SummaryResolution resolution:
        :param list[str] grouping:
        :param int utc_offset:
        """
        self.session = session
        self.query = query
        self.resolution = resolution
        self.grouping = grouping
        self.utc_offset = utc_offset
        self.fill_range = None

    def fill(self, fill_range):
        """
        :param DateTimeRange fill_range:
        :rtype: MeasurementSummary
        """
        self.fill_range = fill_range
        return self

    @property
    def labels(self):
        """
        :rtype list[str]:
        """
        if self.resolution == SummaryResolution.all:
            return [self.ALL_TIME_LABEL]
        if self.fill_range is None:
            return sorted(set(label for label, *g, amount in self.raw_results))
        else:
            return list(LabelRange(
                self.fill_range.begin,
                self.fill_range.end,
                self.resolution,
            ))

    @property
    def groups(self):
        """
        :rtype list[SummaryGroup]:
        """
        groups = []

        for group, results in groupby(self.raw_results, lambda x: x[1:-1]):
            items = {label: amount for label, *g, amount in results}
            groups.append(SummaryGroup(
                group=group,
                values=[items.get(label, None) for label in self.labels],
            ))

        return groups

    @property
    @lru_cache()
    def raw_results(self):
        """
        TODO
        """
        select = []
        groups = []
        orders = []

        q = self.query.subquery()

        # -- Resolution ------------------------------------------------------

        if self.resolution == SummaryResolution.all:
            select.append(bindparam('label', self.ALL_TIME_LABEL))
        else:
            if self.utc_offset is not None:
                b = q.c.begin + text("INTERVAL '%d HOURS'" % self.utc_offset)
            else:
                b = q.c.begin

            select.append(func.to_char(b, self.RESOLUTIONS_POSTGRES[self.resolution]).label('resolution'))
            groups.append('resolution')

        # -- Grouping ------------------------------------------------------------

        for group in self.grouping:
            if group == 'type':
                groups.append(q.c.type)
                select.append(q.c.type)
                orders.append(q.c.type)
            elif group == 'gsrn':
                groups.append(q.c.gsrn)
                select.append(q.c.gsrn)
                orders.append(q.c.gsrn)
            elif group == 'sector':
                groups.append(q.c.sector)
                select.append(q.c.sector)
                orders.append(q.c.sector)
            else:
                raise RuntimeError('Invalid grouping: %s' % self.grouping)

        # -- Query ---------------------------------------------------------------

        select.append(func.sum(q.c.amount))

        return self.session \
            .query(*select) \
            .group_by(*groups) \
            .order_by(*orders) \
            .all()
