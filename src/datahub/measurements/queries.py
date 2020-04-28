import sqlalchemy as sa
from sqlalchemy import func, bindparam
from datetime import datetime
from itertools import groupby
from functools import lru_cache
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import joinedload

from datahub.ggo import Ggo
from datahub.common import DateTimeRange
from datahub.meteringpoints import MeteringPoint, MeasurementType

from .models import (
    Measurement,
    MeasurementFilters,
    SummaryResolution,
    SummaryGroup,
)


class MeasurementQuery(object):
    """
    TODO
    """
    def __init__(self, session, q=None):
        """
        :param Session session:
        :param Query q:
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
        :param MeasurementFilters filters:
        :rtype: MeasurementQuery
        """
        q = self.q

        if filters.gsrn:
            q = q.filter(Measurement.gsrn.in_(filters.gsrn))
        if filters.begin:
            q = q.filter(Measurement.begin == filters.begin)
        elif filters.begin_range:
            q = q.filter(Measurement.begin >= filters.begin_range.begin)
            q = q.filter(Measurement.begin <= filters.begin_range.end)
        if filters.sector:
            q = q.filter(MeteringPoint.sector.in_(filters.sector))
        if filters.type:
            q = q.filter(MeteringPoint.type == filters.type)

        return self.__class__(self.session, q)

    def belongs_to(self, sub):
        """
        TODO

        :param str sub:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def begins_at(self, begin):
        """
        TODO

        :param datetime begin:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.begin == begin,
        ))

    def begins_within(self, begin_range):
        """
        TODO

        :param DateTimeRange begin_range:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(sa.and_(
            Measurement.begin >= begin_range.begin,
            Measurement.begin <= begin_range.end,
        )))

    def has_gsrn(self, gsrn):
        """
        TODO

        :param str gsrn:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.gsrn == gsrn,
        ))

    def is_type(self, type):
        """
        TODO

        :param MeasurementType type:
        :rtype: MeasurementQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.type == type,
        ))

    def is_production(self):
        """
        TODO

        :rtype: MeasurementQuery
        """
        return self.is_type(MeasurementType.PRODUCTION)

    def is_consumption(self):
        """
        TODO

        :rtype: MeasurementQuery
        """
        return self.is_type(MeasurementType.CONSUMPTION)

    def needs_ggo_issued(self):
        """
        TODO

        :rtype: MeasurementQuery
        """
        q = self.q.filter(Ggo.id.is_(None))

        return self.__class__(self.session, q) \
            .is_production()

    def get_distinct_begins(self):
        """
        Returns an iterable of all distinct Ggo.begin
        as a result of this query.

        :rtype: list[datetime]
        """
        return [row[0] for row in self.session.query(
            self.q.subquery().c.begin.distinct())]

    def get_first_measured_begin(self):
        """
        TODO

        :rtype: datetime
        """
        return self.session.query(
            func.min(self.q.subquery().c.begin)).one()[0]

    def get_last_measured_begin(self):
        """
        TODO

        :rtype: datetime
        """
        return self.session.query(
            func.max(self.q.subquery().c.begin)).one()[0]

    def get_summary(self, resolution, grouping):
        """
        :param SummaryResolution resolution:
        :param list[str] grouping:
        :rtype: MeasurementSummary
        """
        return MeasurementSummary(self.session, self, resolution, grouping)


class MeasurementSummary(object):
    """
    TODO Describe
    """

    GROUPINGS = (
        'type',
        'gsrn',
        'sector',
    )

    RESOLUTIONS_POSTGRES = {
        SummaryResolution.HOUR: 'YYYY-MM-DD HH:00',
        SummaryResolution.DAY: 'YYYY-MM-DD',
        SummaryResolution.MONTH: 'YYYY-MM',
        SummaryResolution.YEAR: 'YYYY',
    }

    RESOLUTIONS_PYTHON = {
        SummaryResolution.HOUR: '%Y-%m-%d %H:00',
        SummaryResolution.DAY: '%Y-%m-%d',
        SummaryResolution.MONTH: '%Y-%m',
        SummaryResolution.YEAR: '%Y',
    }

    LABEL_STEP = {
        SummaryResolution.HOUR: relativedelta(hours=1),
        SummaryResolution.DAY: relativedelta(days=1),
        SummaryResolution.MONTH: relativedelta(months=1),
        SummaryResolution.YEAR: relativedelta(years=1),
        SummaryResolution.ALL: None,
    }

    ALL_TIME_LABEL = 'All-time'

    def __init__(self, session, query, resolution, grouping):
        """
        :param Session session:
        :param MeasurementQuery query:
        :param SummaryResolution resolution:
        :param list[str] grouping:
        """
        self.session = session
        self.query = query
        self.resolution = resolution
        self.grouping = grouping
        self.fill_range = None

    def fill(self, fill_range):
        """
        :param DateTimeRange fill_range:
        """
        self.fill_range = fill_range

    @property
    def labels(self):
        """
        :rtype list[str]:
        """
        if self.resolution == SummaryResolution.ALL:
            return [self.ALL_TIME_LABEL]
        if self.fill_range is None:
            return sorted(set(label for label, *g, amount in self.raw_results))
        else:
            format = self.RESOLUTIONS_PYTHON[self.resolution]
            step = self.LABEL_STEP[self.resolution]
            begin = self.fill_range.begin
            labels = []

            while begin < self.fill_range.end:
                labels.append(begin.strftime(format))
                begin += step

            return labels

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

        if self.resolution == SummaryResolution.ALL:
            select.append(bindparam('label', self.ALL_TIME_LABEL))
        else:
            select.append(func.to_char(q.c.begin, self.RESOLUTIONS_POSTGRES[self.resolution]).label('resolution'))
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
