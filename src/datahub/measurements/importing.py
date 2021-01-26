from typing import Dict, Any
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

from datahub import logger
from datahub.ggo import Ggo
from datahub.services import eloverblik as e
from datahub.meteringpoints import MeteringPoint
from datahub.services.energytypes import EnergyTypeService
from datahub.settings import (
    FIRST_MEASUREMENT_TIME,
    LAST_MEASUREMENT_TIME,
    GGO_EXPIRE_TIME,
)

from .models import Measurement
from .queries import MeasurementQuery


# Services
eloverblik_service = e.EloverblikService()
energtype_service = EnergyTypeService()


# Settings
MEASUREMENT_DURATION = timedelta(hours=1)


class MeasurementImporter(object):
    """
    Imports TimeSeries from ElOverblik and converts them
    to Measurement objects.
    """

    def import_measurements(self, gsrn, begin, end):
        """
        datetime_from *INCLUDED*
        datetime_to *EXCLUDED*

        :param str gsrn:
        :param datetime begin:
        :param datetime end:
        :rtype: collections.abc.Iterable[Measurement]
        """

        # The service does not include time series at date=datetime_to,
        # so we add one day to make sure any time series at the date
        # of datetime_to is included in the result
        imported_time_series = eloverblik_service.get_time_series(
            gsrn=gsrn,
            date_from=begin.date(),
            date_to=end.date() + timedelta(days=1),
        )

        # Convert the imported documents to Measurement objects
        imported_measurements = self.flattern_time_series(imported_time_series)

        return (
            m for m in imported_measurements
            if begin <= m.begin.astimezone(timezone.utc) < end
        )

    def flattern_time_series(self, documents):
        """
        :param list[e.TimeSeriesResult] documents:
        :rtype: collections.abc.Iterable[Measurement]
        """
        for d in (_ for _ in documents if _.document is not None):
            for time_series in d.document.time_series:
                unit = time_series.unit

                for period in time_series.period:
                    start = period.time_interval.start

                    for point in period.point:
                        point_start = start + (MEASUREMENT_DURATION * (point.position - 1))
                        point_end = point_start + MEASUREMENT_DURATION

                        yield Measurement(
                            gsrn=time_series.mrid,
                            begin=point_start,
                            end=point_end,
                            amount=int(point.quantity * unit.value),
                            published=False,
                        )


class MeasurementImportController(object):
    """
    TODO
    """

    importer = MeasurementImporter()

    def import_measurements_for(self, meteringpoint, session):
        """
        :param MeteringPoint meteringpoint:
        :param sqlalchemy.orm.Session session:
        :rtype: list[Measurement]
        """
        begin = self.get_begin(meteringpoint, session)
        end = self.get_end()
        measurements = self.get_measurements(
            meteringpoint, begin, end, session)

        # Save measurements to database
        session.add_all(measurements)

        # Issue GGOs if necessary
        if (meteringpoint.is_producer()
                and meteringpoint.technology_code
                and meteringpoint.fuel_code):

            emissions = self.get_emissions(meteringpoint.gsrn)

            session.add_all((
                self.issue_ggo_for(measurement, emissions)
                for measurement in measurements
                if measurement.amount > 0
            ))

        session.flush()

        logger.info(f'Imported {len(measurements)} measurements from ElOverblik for GSRN: {meteringpoint.gsrn}', extra={
            'subject': meteringpoint.sub,
            'gsrn': meteringpoint.gsrn,
            'type': meteringpoint.type.value,
            'begin': str(begin),
            'end': str(end),
        })

        return measurements

    def get_measurements(self, meteringpoint, begin, end, session):
        """
        Imports and filters measurements

        :param MeteringPoint meteringpoint:
        :param datetime begin:
        :param datetime end:
        :param sqlalchemy.orm.Session session:
        :rtype: list[Measurement]
        """

        # Import measurements from ElOverblik
        imported_measurements = self.importer.import_measurements(
            meteringpoint.gsrn, begin, end)

        # Filter out measurements that already exists
        return [
            measurement for measurement in imported_measurements
            if not self.measurement_exists(measurement, session)
        ]

    def get_begin(self, meteringpoint, session):
        """
        :param MeteringPoint meteringpoint:
        :param sqlalchemy.orm.Session session:
        :rtype: datetime
        """
        latest_begin = MeasurementQuery(session) \
            .is_active() \
            .has_gsrn(meteringpoint.gsrn) \
            .get_last_measured_begin()

        if latest_begin:
            # From latest measurement plus the duration of one measurement
            return latest_begin + MEASUREMENT_DURATION
        elif FIRST_MEASUREMENT_TIME:
            # From static defined time
            return FIRST_MEASUREMENT_TIME
        else:
            # From the 1st of the month prior to now
            return self.get_default_begin()

    def get_end(self):
        """
        :rtype: datetime
        """
        if LAST_MEASUREMENT_TIME:
            return min(self.get_default_end(), LAST_MEASUREMENT_TIME)
        else:
            return self.get_default_end()

    def issue_ggo_for(self, measurement, emissions):
        """
        :param Measurement measurement:
        :param Dict[str, Dict[str, Any]] emissions:
        :rtype: Ggo
        """
        return Ggo(
            issue_time=datetime.now(tz=timezone.utc),
            expire_time=datetime.now(tz=timezone.utc) + GGO_EXPIRE_TIME,
            measurement=measurement,
            emissions=emissions,
        )

    def measurement_exists(self, measurement, session):
        """
        :param Measurement measurement:
        :param Session session:
        :rtype: bool
        """
        count = MeasurementQuery(session) \
            .is_active() \
            .has_gsrn(measurement.gsrn) \
            .begins_at(measurement.begin) \
            .count()

        return count > 0

    def get_default_begin(self):
        """
        Returns the 1st of the month prior to now

        :rtype: datetime
        """
        return (datetime.now(tz=timezone.utc) - relativedelta(months=1)) \
            .replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def get_default_end(self):
        """
        Returns today (NOW) at 00:00:00

        :rtype: datetime
        """
        return datetime \
            .fromordinal(date.today().toordinal()) \
            .astimezone(timezone.utc)

    def get_emissions(self, gsrn):
        """
        :param str gsrn:
        :rtype: Dict[str, Dict[str, Any]]
        """
        return energtype_service.get_emissions(gsrn)
