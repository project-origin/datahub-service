import sqlalchemy as sa
from sqlalchemy.orm import joinedload
from datetime import datetime, timezone

from datahub.common import DateTimeRange
from datahub.measurements import Measurement
from datahub.meteringpoints import MeteringPoint

from .models import Ggo


class GgoQuery(object):
    """
    Abstraction around querying Ggo objects from the database,
    supporting cascade calls to combine filters.

    Usage example::

        query = GgoQuery(session) \
            .belongs_to('65e58f0b-62dd-40c2-a540-f773b0beed66') \
            .begins_at(datetime(2020, 1, 1, 0, 0))

        for ggo in query:
            pass

    Attributes not present on the GgoQuery class is redirected to
    SQLAlchemy's Query object, like count(), all() etc., for example::

        query = GgoQuery(session) \
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
        Only include GGOs which belong to the user identified by
        the provided sub (subject).

        :param str sub:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def begins_at(self, begin):
        """
        Only include GGOs which begins at the provided datetime.

        :param datetime begin:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.begin == begin.astimezone(timezone.utc),
        ))

    def begins_within(self, begin_range):
        """
        Only include GGOs which begins within the provided datetime
        range (both begin and end are included).

        :param DateTimeRange begin_range:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(sa.and_(
            Measurement.begin >= begin_range.begin.astimezone(timezone.utc),
            Measurement.begin <= begin_range.end.astimezone(timezone.utc),
        )))

    def has_gsrn(self, gsrn):
        """
        Only include GGOs which were issued to the MeteringPoint
        identified with the provided GSRN number.

        :param str gsrn:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.gsrn == gsrn,
        ))

    def is_published(self, value=True):
        """
        Only include GGOs which has been published to the ledger (and hence
        are publicly available).

        :param bool value:
        :rtype: GgoQuery
        """
        return self.__class__(self.session, self.q.filter(
            Measurement.published == value,
        ))
