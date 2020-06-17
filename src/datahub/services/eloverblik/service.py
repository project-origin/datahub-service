import json

import marshmallow
import requests
import marshmallow_dataclass as md
from functools import partial

from datahub.cache import redis
from datahub.settings import ELOVERBLIK_SERVICE_URL, ELOVERBLIK_TOKEN, DEBUG

from .models import (
    Scope,
    MeteringPoint,
    GetTokenResponse,
    GetMeteringPointsResponse,
    TimeSeriesResult,
    GetTimeSeriesResponse,
)


TOKEN_EXPIRE = 3600


class EloverblikServiceConnectionError(Exception):
    """
    Raised when publishing an event to a webhook results
    in a connection error
    """
    pass


class EloverblikServiceError(Exception):
    """
    Raised when invoking DataHubService results in a status code != 200
    """
    def __init__(self, message, status_code, response_body):
        super(EloverblikServiceError, self).__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class EloverblikService(object):
    """
    Interface for importing data from ElOverblik.
    """
    def invoke(self, f, token, path, response_schema):
        """
        :param function f:
        :param str token:
        :param str path:
        :param Schema response_schema:
        :rtype obj:
        """
        url = '%s%s' % (ELOVERBLIK_SERVICE_URL, path)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-type': 'application/json',
            'accept': 'application/json',
        }

        try:
            response = f(
                url=url,
                verify=not DEBUG,
                headers=headers,
            )
        except:
            raise EloverblikServiceConnectionError(
                'Failed request to Eloverblik')

        if response.status_code != 200:
            raise EloverblikServiceError(
                (
                    f'Invoking ElOverblik resulted in status code {response.status_code}: '
                    f'{url}\n\n{response.content}'
                ),
                status_code=response.status_code,
                response_body=str(response.content),
            )

        try:
            response_json = response.json()
            response_model = response_schema().load(response_json)
        except json.decoder.JSONDecodeError:
            raise EloverblikServiceError(
                f'Failed to parse response JSON: {url}\n\n{response.content}',
                status_code=response.status_code,
                response_body=str(response.content),
            )
        except marshmallow.ValidationError:
            raise EloverblikServiceError(
                f'Failed to validate response JSON: {url}\n\n{response.content}',
                status_code=response.status_code,
                response_body=str(response.content),
            )

        return response_model

    def get(self, *args, **kwargs):
        return self.invoke(requests.get, *args, **kwargs)

    def post(self, body, *args, **kwargs):
        return self.invoke(partial(requests.post, json=body), *args, **kwargs)

    def get_token(self):
        """
        Get a temporary access token, which can be used in subsequent
        calls to the service.

        :rtype: str
        """
        token = redis.get('eloverblik-token')

        if token is None:
            response = self.get(
                token=ELOVERBLIK_TOKEN,
                path='/api/Token',
                response_schema=md.class_schema(GetTokenResponse),
            )
            token = response.result

            redis.set('eloverblik-token', token, ex=TOKEN_EXPIRE)
        else:
            token = token.decode()

        return token

    def get_meteringpoints(self, scope, identifier):
        """
        Get a list of MeteringPoints.

        :param Scope scope:
        :param str identifier:
        :rtype: list[MeteringPoint]
        """
        response = self.get(
            token=self.get_token(),
            path=f'/api/Authorization/Authorization/MeteringPoints/{scope.value}/{identifier}',
            response_schema=md.class_schema(GetMeteringPointsResponse),
        )

        return response.result

    def get_time_series(self, gsrn, date_from, date_to):
        """
        Get a list of TimeSeries.

        :param str gsrn:
        :param datetime.date date_from:
        :param datetime.date date_to:
        :rtype: list[TimeSeriesResult]
        """
        body = {
            'meteringPoints':  {
                'meteringPoint': [gsrn]
            }
        }

        date_from_formatted = date_from.strftime('%Y-%m-%d')
        date_to_formatted = date_to.strftime('%Y-%m-%d')

        response = self.post(
            body=body,
            token=self.get_token(),
            path=f'/api/MeterData/GetTimeSeries/{date_from_formatted}/{date_to_formatted}/Hour',
            response_schema=md.class_schema(GetTimeSeriesResponse),
        )

        return response.result
