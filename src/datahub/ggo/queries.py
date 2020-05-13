import sqlalchemy as sa
from sqlalchemy.orm import joinedload
from datetime import datetime

from datahub.common import DateTimeRange
from datahub.measurements import Measurement
from datahub.meteringpoints import MeteringPoint

from .models import Ggo


class GgoQuery(object):
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
            self.q = session.query(Ggo) \
                .join(Measurement, Measurement.id == Ggo.measurement_id) \
                .join(MeteringPoint, MeteringPoint.gsrn == Measurement.gsrn) \
                .options(joinedload(Ggo.measurement))

    def __iter__(self):
        return iter(self.q)

    def __getattr__(self, name):
        return getattr(self.q, name)

    def belongs_to(self, sub):
        """
        TODO

        :param str sub:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def begins_at(self, begin):
        """
        TODO

        :param datetime begin:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.begin == begin,
        ))

    def begins_within(self, begin_range):
        """
        TODO

        :param DateTimeRange begin_range:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(sa.and_(
            Measurement.begin >= begin_range.begin,
            Measurement.begin <= begin_range.end,
        )))

    def has_gsrn(self, gsrn):
        """
        TODO

        :param str gsrn:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.gsrn == gsrn,
        ))

    def is_published(self, value=True):
        """
        :param bool value:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.published == value,
        ))
