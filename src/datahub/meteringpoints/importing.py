from functools import partial

from datahub import logger
from datahub.services import eloverblik as e

from .queries import MeteringPointQuery
from .models import MeteringPoint, MeasurementType


# Services
eloverblik_service = e.EloverblikService()


class MeteringPointImporter(object):
    """
    Helper class for importing MeteringPoints from ElOverblik.
    """
    def import_meteringpoints(self, sub, session):
        """
        Imports all meteringpoints for the subject from ElOverblik.

        :param str sub:
        :param sqlalchemy.orm.Session session:
        :rtype: list[MeteringPoint]
        """
        logger.info(f'Importing MeteringPoints from ElOverblik', extra={
            'subject': sub,
        })

        # Import MeteringPoints from ElOverblik
        imported_meteringpoints = eloverblik_service.get_meteringpoints(
            scope=e.Scope.CustomerKey,
            identifier=sub,
        )

        # Filter out GSRN numbers that already exists
        filtered_meteringpoints = (
            mp for mp in imported_meteringpoints
            if not self.gsrn_exists(mp.gsrn, session)
        )

        # Map to MeteringPoint objects
        mapped_meteringpoints = list(map(
            partial(self.map_imported_meteringpoint, sub),
            filtered_meteringpoints,
        ))

        # Insert to database
        session.add_all(mapped_meteringpoints)
        session.flush()

        logger.info(f'Imported {len(mapped_meteringpoints)} MeteringPoints from ElOverblik', extra={
            'subject': sub,
            'gsrn': ', '.join(m.gsrn for m in mapped_meteringpoints),
        })

        return mapped_meteringpoints

    def gsrn_exists(self, gsrn, session):
        """
        Check whether a GSRN number already exists (has been imported).

        :param str gsrn:
        :param sqlalchemy.orm.Session session:
        :rtype: bool
        """
        count = MeteringPointQuery(session) \
            .has_gsrn(gsrn) \
            .count()

        return count > 0

    def map_imported_meteringpoint(self, sub, imported_meteringpoint):
        """
        Maps the MeteringPoint datatype returned by ElOverblikService to
        a MeteringPoint object ready for database insertion.

        :param str sub:
        :param e.MeteringPoint imported_meteringpoint:
        :rtype: MeteringPoint
        """
        return MeteringPoint(
            sub=sub,
            gsrn=imported_meteringpoint.gsrn,
            type=self.get_type(imported_meteringpoint),
            sector=self.get_sector(imported_meteringpoint),
            technology_code=None,
            fuel_code=None,
            street_code=imported_meteringpoint.street_code,
            street_name=imported_meteringpoint.street_name,
            building_number=imported_meteringpoint.building_number,
            city_name=imported_meteringpoint.city_name,
            postcode=imported_meteringpoint.postcode,
            municipality_code=imported_meteringpoint.municipality_code,
        )

    def get_type(self, imported_meteringpoint):
        """
        Maps the type (production or consumption) returned by
        ElOverblikService to the type required by Measurement class.

        :param e.MeteringPoint imported_meteringpoint:
        :rtype: MeasurementType
        """
        if imported_meteringpoint.type_of_mp is e.MeterPointType.PRODUCTION:
            return MeasurementType.PRODUCTION
        elif imported_meteringpoint.type_of_mp is e.MeterPointType.CONSUMPTION:
            return MeasurementType.CONSUMPTION
        else:
            raise RuntimeError('Should NOT have happened!')

    def get_sector(self, imported_meteringpoint):
        """
        Deduces the sector (price area) for a MeteringPoint based on the
        data returned from ElOverblikService.

        :param e.MeteringPoint imported_meteringpoint:
        :rtype: str
        """
        if int(imported_meteringpoint.postcode) < 5000:
            return 'DK2'
        else:
            return 'DK1'
