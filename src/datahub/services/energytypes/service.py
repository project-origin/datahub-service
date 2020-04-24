import requests

from datahub.settings import ENERGY_TYPE_SERVICE_URL


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
        )

        response_json = response.json()

        return (
            response_json['technologyCode'],
            response_json['fuelCode'],
        )
