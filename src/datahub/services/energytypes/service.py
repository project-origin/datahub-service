import json
import requests

from datahub.settings import ENERGY_TYPE_SERVICE_URL, DEBUG


class EnergyTypeServiceConnectionError(Exception):
    """
    Raised when invoking EnergyTypeService results
    in a connection error
    """
    pass


class EnergyTypeServiceError(Exception):
    """
    Raised when invoking EnergyTypeService results
    in a status code != 200
    """
    def __init__(self, message, status_code, response_body):
        super(EnergyTypeServiceError, self).__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class EnergyTypeUnavailable(Exception):
    """
    Raised when requesting energy type which is unavailable
    for the requested GSRN
    """
    pass


class EnergyTypeService(object):
    """
    Interface for importing data from EnergyTypeService.
    """
    def get_energy_type(self, gsrn):
        """
        Returns a tuple of (technology code, fuel code) for a MeteringPoint.

        :param str gsrn:
        :rtype (str, str):
        :return: A tuple of (technologyCode, fuelCode)
        """
        url = f'{ENERGY_TYPE_SERVICE_URL}/get-energy-type'

        try:
            response = requests.get(
                url=url,
                params={'gsrn': gsrn},
                verify=not DEBUG,
            )
        except:
            raise EnergyTypeServiceConnectionError(
                'Failed request to EnergyTypeService')

        if response.status_code != 200:
            raise EnergyTypeServiceError(
                (
                    f'Invoking EnergyTypeService resulted in status code {response.status_code}: '
                    f'{url}\n\n{response.content}'
                ),
                status_code=response.status_code,
                response_body=str(response.content),
            )

        try:
            response_json = response.json()
        except json.decoder.JSONDecodeError:
            raise EnergyTypeServiceError(
                f'Failed to parse response JSON: {url}\n\n{response.content}',
                status_code=response.status_code,
                response_body=str(response.content),
            )

        if response_json.get('success') is not True:
            raise EnergyTypeUnavailable(
                response_json.get('message', f'Failed to resolve energy type for GSRN {gsrn}'))

        return (
            response_json['technologyCode'],
            response_json['fuelCode'],
        )
