import requests

from datahub import logger
from datahub.settings import ENERGY_TYPE_SERVICE_URL, DEBUG


class EnergyTypeService(object):
    """
    TODO
    """

    def get_energy_type(self, gsrn):
        """
        :param str gsrn:
        :rtype (str, str):
        :return: A tuple of (technologyCode, fuelCode)
        """
        response = requests.get(
            url=f'{ENERGY_TYPE_SERVICE_URL}/get-energy-type',
            params={'gsrn': gsrn},
            verify=not DEBUG,
        )

        if response.status_code == 404:
            logger.warning('Could not find fuel or tech code for GRSN', extra={'gsrn': gsrn})
            raise Exception('Could not find fuel or tech code for GRSN')

        if response.status_code != 200:
            raise Exception('%d\n\n%s\n\n' % (response.status_code, response.content))

        response_json = response.json()

        return (
            response_json['technologyCode'],
            response_json['fuelCode'],
        )
