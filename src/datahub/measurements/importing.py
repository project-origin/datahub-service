import logging
from datetime import date, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

from datahub import logger
from datahub.ggo import Ggo
from datahub.db import atomic
from datahub.services import eloverblik as e
from datahub.meteringpoints import MeteringPoint, MeasurementType
from datahub.settings import FIRST_MEASUREMENT_TIME, GGO_EXPIRE_TIME

from .models import Measurement
from .queries import MeasurementQuery


# Services
eloverblik_service = e.EloverblikService()


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
        imported_measurements = list(self.flattern_time_series(imported_time_series))

        return (
            m for m in imported_measurements
            if begin <= m.begin.astimezone(timezone.utc) < end
        )

    def flattern_time_series(self, documents):
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

        logger.info(f'Importing measurements from ElOverblik for GSRN: {meteringpoint.gsrn}', extra={
            'subject': meteringpoint.sub,
            'gsrn': meteringpoint.gsrn,
            'type': meteringpoint.type.value,
            'begin': str(begin),
            'end': str(end),
        })

        # Import measurements from ElOverblik
        imported_measurements = self.importer.import_measurements(
            meteringpoint.gsrn, begin, end)

        # Filter out measurements that already exists
        filtered_measurements = [
            measurement for measurement in imported_measurements
            if not self.measurement_exists(measurement, session)
        ]

        # Save measurements to database
        session.add_all(filtered_measurements)

        # Issue GGOs if necessary
        if meteringpoint.is_producer():
            session.add_all((
                self.issue_ggo_for(measurement)
                for measurement in filtered_measurements
            ))

        session.flush()

        logger.info(f'Imported {len(filtered_measurements)} measurements from ElOverblik for GSRN: {meteringpoint.gsrn}', extra={
            'subject': meteringpoint.sub,
            'gsrn': meteringpoint.gsrn,
            'type': meteringpoint.type.value,
            'begin': str(begin),
            'end': str(end),
        })

        return filtered_measurements

    def get_begin(self, meteringpoint, session):
        """
        :param MeteringPoint meteringpoint:
        :param sqlalchemy.orm.Session session:
        :rtype: datetime
        """
        latest_begin = MeasurementQuery(session) \
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
            return (datetime.now(tz=timezone.utc) - relativedelta(months=1)) \
                .replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def get_end(self):
        """
        :rtype: datetime
        """
        return datetime \
            .fromordinal(date.today().toordinal()) \
            .astimezone(timezone.utc)

    def issue_ggo_for(self, measurement):
        """
        :param Measurement measurement:
        :rtype: Ggo
        """
        return Ggo(
            issue_time=datetime.now(tz=timezone.utc),
            expire_time=datetime.now(tz=timezone.utc) + GGO_EXPIRE_TIME,
            measurement=measurement,
        )

    def measurement_exists(self, measurement, session):
        """
        :param Measurement measurement:
        :param Session session:
        :rtype: bool
        """
        count = MeasurementQuery(session) \
            .has_gsrn(measurement.gsrn) \
            .begins_at(measurement.begin) \
            .count()

        return count > 0


# class MeasurementImporter(object):
#     """
#     Helper class for importing Measurements from ElOverblik and issuing
#     GGOs for a specific MeteringPoint.
#
#     If measurements already exists for the MeteringPoint, then only
#     newer measurements are imported (continues where data stopped).
#     Otherwise it imports measurements from "day 1" (defined by
#     settings.FIRST_MEASUREMENT_TIME).
#
#     Issues GGOs where necessary (ie. if MeteringPoint is type PRODUCTION).
#
#     TODO Refactor and test this class
#     """
#     MEASUREMENT_DURATION = timedelta(hours=1)
#
#     @atomic
#     def import_measurements_for(self, meteringpoint, session):
#         """
#         Imports measurements, and optionally issued GGOs,
#         for the provided MeteringPoint.
#
#         :param MeteringPoint meteringpoint:
#         :param Session session:
#         :rtype: list[Measurement]
#         """
#
#         # -- Datetime from and to --------------------------------------------
#
#         latest_begin = MeasurementQuery(session) \
#             .has_gsrn(meteringpoint.gsrn) \
#             .get_last_measured_begin()
#
#         # From latest measurement plus one hour
#         if latest_begin:
#             datetime_from = latest_begin + self.MEASUREMENT_DURATION
#         elif FIRST_MEASUREMENT_TIME:
#             datetime_from = datetime.strptime(FIRST_MEASUREMENT_TIME, "%Y-%m-%dT%H:%M:%SZ").astimezone(timezone.utc)
#         else:
#             datetime_from = (datetime.now() - relativedelta(months=1)) \
#                 .replace(day=1, hour=0, minute=0, second=0, microsecond=0)
#
#         # Up until and including yesterday (datetime_to is excluded)
#         datetime_to = datetime.fromordinal(date.today().toordinal())
#
#         # -- Import from ElOverblik ------------------------------------------
#
#         logger.info(f'Importing measurements from ElOverblik for GSRN: {meteringpoint.gsrn}', extra={
#             'subject': meteringpoint.sub,
#             'gsrn': meteringpoint.gsrn,
#             'type': meteringpoint.type.value,
#             'datetime_from': str(datetime_from),
#             'datetime_to': str(datetime_to),
#         })
#
#         # Imported measurements
#         imported_measurements = self.import_measurements(
#             meteringpoint=meteringpoint,
#             datetime_from=datetime_from.replace(tzinfo=timezone.utc),
#             datetime_to=datetime_to.replace(tzinfo=timezone.utc),
#         )
#
#         # -- Filter, insert to DB, and issue Ggos --------------------------------------
#
#         measurements = []
#
#         # Save measurements to DB and issue GGOs if necessary
#         for measurement in imported_measurements:
#             if self.meaurement_exists(measurement, session):
#                 logging.error('Measurement already exists: %s' % str(measurement))
#                 continue
#
#             session.add(measurement)
#             measurements.append(measurement)
#
#             if meteringpoint.type is MeasurementType.PRODUCTION:
#                 session.add(self.issue_ggo_for(measurement))
#
#             session.flush()
#
#         logger.info(f'Imported {len(measurements)} measurements from ElOverblik for GSRN: {meteringpoint.gsrn}', extra={
#             'subject': meteringpoint.sub,
#             'gsrn': meteringpoint.gsrn,
#             'type': meteringpoint.type.value,
#             'datetime_from': str(datetime_from),
#             'datetime_to': str(datetime_to),
#         })
#
#         return measurements
#
#     def import_measurements(self, meteringpoint, datetime_from, datetime_to):
#         """
#         datetime_from *INCLUDED*
#         datetime_to *EXCLUDED*
#
#         :param MeteringPoint meteringpoint:
#         :param datetime datetime_from:
#         :param datetime datetime_to:
#         :rtype: list[Measurement]
#         """
#
#         # The service does not include time series at date=datetime_to,
#         # so we add one day to make sure any time series at the date
#         # of datetime_to is included in the result
#         imported_time_series = eloverblik_service.get_time_series(
#             gsrn=meteringpoint.gsrn,
#             date_from=datetime_from.date(),
#             date_to=datetime_to.date() + timedelta(days=1),
#         )
#
#         # Convert the imported documents to Measurement objects
#         measurements = self.flattern_imported_time_series(imported_time_series)
#
#         return [
#             m for m in measurements
#             if datetime_from <= m.begin < datetime_to
#         ]
#
#     def flattern_imported_time_series(self, documents):
#         """
#         :param list[e.TimeSeriesResult] documents:
#         :rtype: collections.abc.Iterable[Measurement]
#         """
#         for d in documents:
#             for time_series in d.document.time_series:
#                 unit = time_series.unit
#
#                 for period in time_series.period:
#                     start = period.time_interval.start
#
#                     for point in period.point:
#                         point_start = start + (self.MEASUREMENT_DURATION * (point.position - 1))
#                         point_end = point_start + self.MEASUREMENT_DURATION
#
#                         yield Measurement(
#                             gsrn=time_series.mrid,
#                             begin=point_start,
#                             end=point_end,
#                             amount=int(point.quantity * unit.value),
#                             published=False,
#                         )
#
#     def issue_ggo_for(self, measurement):
#         """
#         :param Measurement measurement:
#         :rtype: Ggo
#         """
#         return Ggo(
#             issue_time=datetime.now(tz=timezone.utc),
#             expire_time=datetime.now(tz=timezone.utc) + GGO_EXPIRE_TIME,
#             measurement=measurement,
#         )
#
#     def meaurement_exists(self, measurement, session):
#         """
#         :param Measurement measurement:
#         :param Session session:
#         :rtype: bool
#         """
#         count = MeasurementQuery(session) \
#             .has_gsrn(measurement.gsrn) \
#             .begins_at(measurement.begin) \
#             .count()
#
#         return count > 0
