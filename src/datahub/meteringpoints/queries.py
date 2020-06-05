from .models import MeteringPoint, MeasurementType


class MeteringPointQuery(object):
    """
    Abstraction around querying MeteringPoint objects from the database,
    supporting cascade calls to combine filters.

    Usage example::

        query = MeteringPoint(session) \
            .belongs_to('65e58f0b-62dd-40c2-a540-f773b0beed66') \
            .is_production()

        for meteringpoint in query:
            pass

    Attributes not present on the GgoQuery class is redirected to
    SQLAlchemy's Query object, like count(), all() etc., for example::

        query = MeteringPoint(session) \
            .belongs_to('65e58f0b-62dd-40c2-a540-f773b0beed66') \
            .is_production() \
            .offset(100) \
            .limit(20) \
            .count()
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
        Only include meteringpoints which belong to the user identified by
        the provided sub (subject).

        :param str sub:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.sub == sub,
        ))

    def has_gsrn(self, gsrn):
        """
        Only include the meteringpoint with the provided GSRN number.

        :param str gsrn:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.gsrn == gsrn,
        ))

    def has_key(self):
        """
        Only return meteringpoints which has their key set.

        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.key.isnot(None),
        ))

    def is_type(self, type):
        """
        Only include meteringpoints of the provided type,
        ie. PRODUCTION or CONSUMPTION.

        :param MeasurementType type:
        :rtype: MeteringPointQuery
        """
        return self.__class__(self.session, self.q.filter(
            MeteringPoint.type == type,
        ))

    def is_production(self):
        """
        Only include meteringpoints of type PRODUCTION.

        :rtype: MeteringPointQuery
        """
        return self.is_type(MeasurementType.PRODUCTION)

    def is_consumption(self):
        """
        Only include meteringpoints of type CONSUMPTION.

        :rtype: MeteringPointQuery
        """
        return self.is_type(MeasurementType.CONSUMPTION)
