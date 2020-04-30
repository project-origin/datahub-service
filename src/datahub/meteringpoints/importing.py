from datahub import logger
from datahub.db import atomic
from datahub.services import eloverblik as e
from datahub.services.energytypes import EnergyTypeService

from .queries import MeteringPointQuery
from .models import MeteringPoint, MeasurementType


class MeteringPointsImportController(object):
    """
    TODO
    """
    service = e.EloverblikService()
    energtypes = EnergyTypeService()

    def import_meteringpoints(self, sub):
        """
        :param str sub:
        :rtype: list[MeteringPoint]
        """
        logger.info(f'Importing MeteringPoints from ElOverblik', extra={
            'subject': sub,
        })

        imported_meteringpoints = self.service.get_meteringpoints(
            scope=e.Scope.CustomerKey,
            identifier=sub,
        )

        mapped_meteringpoints = self.insert_to_db(
            sub, imported_meteringpoints)

        logger.info(f'Imported {len(mapped_meteringpoints)} MeteringPoints from ElOverblik', extra={
            'subject': sub,
            'gsrn': ', '.join(m.gsrn for m in mapped_meteringpoints),
        })

        return mapped_meteringpoints

    @atomic
    def insert_to_db(self, sub, imported_meteringpoints, session):
        """
        :param str sub:
        :param list[e.MeteringPoint] imported_meteringpoints:
        :param Session session:
        """
        meteringpoints = []

        for i, imported_meteringpoint in enumerate(imported_meteringpoints):
            count = MeteringPointQuery(session) \
                .has_gsrn(imported_meteringpoint.gsrn) \
                .count()

            if count == 0:
                meteringpoints.append(self.map_imported_meteringpoint(
                    sub, imported_meteringpoint))

        session.add_all(meteringpoints)

        return meteringpoints

    def map_imported_meteringpoint(self, sub, imported_meteringpoint):
        """
        :param str sub:
        :param e.MeteringPoint imported_meteringpoint:
        :rtype: MeteringPoint
        """
        technology_code, fuel_code = self.energtypes \
            .get_energy_type(imported_meteringpoint.gsrn)

        return MeteringPoint(
            sub=sub,
            gsrn=imported_meteringpoint.gsrn,
            type=self.get_type(imported_meteringpoint),
            sector=self.get_sector(imported_meteringpoint),
            technology_code=technology_code,
            fuel_code=fuel_code,
            street_code=imported_meteringpoint.street_code,
            street_name=imported_meteringpoint.street_name,
            building_number=imported_meteringpoint.building_number,
            city_name=imported_meteringpoint.city_name,
            postcode=imported_meteringpoint.postcode,
            municipality_code=imported_meteringpoint.municipality_code,
        )

    def get_type(self, imported_meteringpoint):
        """
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
        :param e.MeteringPoint imported_meteringpoint:
        :rtype: str
        """
        if int(imported_meteringpoint.postcode) < 5000:
            return 'DK2'
        else:
            return 'DK1'
