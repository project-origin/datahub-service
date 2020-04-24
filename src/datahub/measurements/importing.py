import logging
from sqlalchemy.exc import IntegrityError
from datetime import date, datetime, timedelta, timezone

from datahub.db import atomic, inject_session
from datahub.services import eloverblik as e
from datahub.settings import FIRST_MEASUREMENT_TIME
from datahub.meteringpoints import MeteringPoint

from .models import Measurement
from .queries import MeasurementQuery


class MeasurementImportController(object):
    """
    TODO
    """
    MEASUREMENT_DURATION = timedelta(hours=1)

    service = e.EloverblikService()

    @inject_session
    def import_measurements_for(self, meteringpoint, session):
        """
        :param MeteringPoint meteringpoint:
        :param Session session:
        :rtype: list[Measurement]
        """
        latest_begin = MeasurementQuery(session) \
            .has_gsrn(meteringpoint.gsrn) \
            .get_last_measured_begin()

        # From latest measurement plus one hour
        datetime_from = latest_begin + self.MEASUREMENT_DURATION \
            if latest_begin \
            else FIRST_MEASUREMENT_TIME

        # Up until and including yesterday (datetime_to is excluded)
        datetime_to = datetime.fromordinal(date.today().toordinal())
        # datetime_to = datetime_to.replace(tzinfo=timezone.utc)

        return self.import_measurements(
            gsrn=meteringpoint.gsrn,
            datetime_from=datetime_from.replace(tzinfo=timezone.utc),
            datetime_to=datetime_to.replace(tzinfo=timezone.utc),
        )

    @atomic
    def import_measurements(self, gsrn, datetime_from, datetime_to, session):
        """
        datetime_from *INCLUDED*
        datetime_to *EXCLUDED*

        :param str gsrn:
        :param datetime datetime_from:
        :param datetime datetime_to:
        :param Session session:
        :rtype: list[Measurement]
        """

        # The service does not include time series at date=datetime_to,
        # so we add one day to make sure any time series at the date
        # of datetime_to is included in the result
        imported_documents = self.service.get_time_series(
            gsrn=gsrn,
            date_from=datetime_from.date(),
            date_to=datetime_to.date() + timedelta(days=1),
        )

        measurements = []

        for measurement in self.flattern_to_measurements(imported_documents):
            if datetime_from <= measurement.begin < datetime_to:
                count = MeasurementQuery(session) \
                    .has_gsrn(measurement.gsrn) \
                    .begins_at(measurement.begin) \
                    .count()

                if count == 0:
                    measurements.append(measurement)
                    session.add(measurement)
                    session.flush()
                else:
                    logging.error('Duplicate pair of (GSRN, Begin) for measurement: %s' % str(measurement))

        return measurements

    def flattern_to_measurements(self, documents):
        """
        :param list[e.TimeSeriesResult] documents:
        :rtype: collections.abc.Iterable[Measurement]
        """
        for d in documents:
            for time_series in d.document.time_series:
                unit = time_series.unit

                for period in time_series.period:
                    start = period.time_interval.start

                    for point in period.point:
                        point_start = start + (self.MEASUREMENT_DURATION * (point.position - 1))
                        point_end = point_start + self.MEASUREMENT_DURATION

                        yield Measurement(
                            gsrn=time_series.mrid,
                            begin=point_start,
                            end=point_end,
                            amount=int(point.quantity * unit.value)
                        )
