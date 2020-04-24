from sqlalchemy import func, bindparam
from datetime import datetime
from itertools import groupby
from functools import lru_cache
from dateutil.relativedelta import relativedelta

from .models import (
    MeteringPoint,
    MeasurementType,
)


class MeteringPointQuery(object):
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
            self.q = session.query(MeteringPoint)

    def __iter__(self):
        return iter(self.q)

    def __getattr__(self, name):
        return getattr(self.q, name)

    def belongs_to(self, sub):
        """
        TODO

        :param str sub:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def has_gsrn(self, gsrn):
        """
        TODO

        :param str gsrn:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.gsrn == gsrn,
        ))

    def has_key(self):
        """
        TODO

        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.key.isnot(None),
        ))

    def is_type(self, type):
        """
        TODO

        :param MeasurementType type:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.type == type,
        ))

    def is_production(self):
        """
        TODO

        :rtype: MeteringPointQuery
        """
        return self.is_type(MeasurementType.PRODUCTION)

    def is_consumption(self):
        """
        TODO

        :rtype: MeteringPointQuery
        """
        return self.is_type(MeasurementType.CONSUMPTION)
