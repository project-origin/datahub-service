import sqlalchemy as sa
from functools import lru_cache
from itertools import groupby
from dateutil.relativedelta import relativedelta

from datahub.measurements import Measurement
from datahub.common import (
    DateTimeRange,
    SummaryResolution,
    SummaryGroup,
    LabelRange,
)

from .models import DisclosureRetiredGgo, DisclosureSettlement, Disclosure


class DisclosureRetiredGgoQuery(object):
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
            self.q = session.query(DisclosureRetiredGgo) \
                .join(DisclosureSettlement, DisclosureSettlement.id == DisclosureRetiredGgo.settlement_id) \
                .join(Disclosure, Disclosure.id == DisclosureSettlement.disclosure_id) \
                .join(Measurement, Measurement.id == DisclosureSettlement.measurement_id)

    def __iter__(self):
        return iter(self.q)

    def __getattr__(self, name):
        return getattr(self.q, name)

    def from_disclosure(self, disclosure):
        """
        TODO

        :param Disclosure disclosure:
        :rtype: DisclosureRetiredGgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Disclosure.id == disclosure.id,
        ))

    def has_gsrn(self, gsrn):
        """
        TODO

        :param str gsrn:
        :rtype: DisclosureRetiredGgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.gsrn == gsrn,
        ))

    def begins_within(self, begin_range):
        """
        TODO

        :param DateTimeRange begin_range:
        :rtype: DisclosureRetiredGgoQuery
        """
        return self.__class__(self.session, self.q.filter(sa.and_(
            DisclosureRetiredGgo.begin >= begin_range.begin,
            DisclosureRetiredGgo.begin <= begin_range.end,
        )))

    def get_summary(self, resolution, grouping):
        """
        :param SummaryResolution resolution:
        :param list[str] grouping:
        :rtype: DisclosureRetiredGgoSummary
        """
        return DisclosureRetiredGgoSummary(self.session, self, resolution, grouping)


class DisclosureRetiredGgoSummary(object):
    """
    TODO Describe
    """

    GROUPINGS = (
        'begin',
        'sector',
        'technologyCode',
        'fuelCode',
    )

    RESOLUTIONS_POSTGRES = {
        SummaryResolution.hour: 'YYYY-MM-DD HH24:00',
        SummaryResolution.day: 'YYYY-MM-DD',
        SummaryResolution.month: 'YYYY-MM',
        SummaryResolution.year: 'YYYY',
    }

    RESOLUTIONS_PYTHON = {
        SummaryResolution.hour: '%Y-%m-%d %H:00',
        SummaryResolution.day: '%Y-%m-%d',
        SummaryResolution.month: '%Y-%m',
        SummaryResolution.year: '%Y',
    }

    LABEL_STEP = {
        SummaryResolution.hour: relativedelta(hours=1),
        SummaryResolution.day: relativedelta(days=1),
        SummaryResolution.month: relativedelta(months=1),
        SummaryResolution.year: relativedelta(years=1),
        SummaryResolution.all: None,
    }

    ALL_TIME_LABEL = 'All-time'

    def __init__(self, session, query, resolution, grouping):
        """
        :param Session session:
        :param DisclosureRetiredGgoQuery query:
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
        :rtype: DisclosureRetiredGgoSummary
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
            select.append(sa.bindparam('label', self.ALL_TIME_LABEL))
        else:
            select.append(sa.func.to_char(q.c.begin, self.RESOLUTIONS_POSTGRES[self.resolution]).label('resolution'))
            groups.append('resolution')

        # -- Grouping ------------------------------------------------------------

        for group in self.grouping:
            if group == 'begin':
                groups.append(q.c.begin)
                select.append(q.c.begin)
                orders.append(q.c.begin)
            elif group == 'sector':
                groups.append(q.c.sector)
                select.append(q.c.sector)
                orders.append(q.c.sector)
            elif group == 'technologyCode':
                groups.append(q.c.technology_code)
                select.append(q.c.technology_code)
                orders.append(q.c.technology_code)
            elif group == 'fuelCode':
                groups.append(q.c.fuel_code)
                select.append(q.c.fuel_code)
                orders.append(q.c.fuel_code)
            else:
                raise RuntimeError('Invalid grouping: %s' % self.grouping)

        # -- Query ---------------------------------------------------------------

        select.append(sa.func.sum(q.c.amount))

        return self.session \
            .query(*select) \
            .group_by(*groups) \
            .order_by(*orders) \
            .all()
