import requests
import marshmallow_dataclass as md
from functools import partial

from datahub import logger
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

        try:
            response = f(
                url='%s%s' % (ELOVERBLIK_SERVICE_URL, path),
                verify=not DEBUG,
                headers={
                    'Authorization': f'Bearer {token}',
                    'Content-type': 'application/json',
                    'accept': 'application/json',
                },
            )

            if response.status_code != 200:
                raise Exception((
                    'Invoking ElOverblik resulted status_code != 200: '
                    '%s %d\n\n%s\n\n'
                ) % (url, response.status_code, response.content))
        except:
            logger.exception('Invoking ElOverblik resulted in an exception', extra={
                'url': url,
            })
            raise

        try:
            response_json = response.json()
            response_model = response_schema().load(response_json)
        except:
            logger.exception('Failed to convert ElOverblik JSON into model using the provided schema', extra={
                'url': url,
                'content': response.content,
            })
            raise

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
        with logger.tracer.span('Get from redis cache'):
            token = redis.get('eloverblik-token')

        if token is None:
            with logger.tracer.span('Fetching token from ElOverblik'):
                response = self.get(
                    token=ELOVERBLIK_TOKEN,
                    path='/api/Token',
                    response_schema=md.class_schema(GetTokenResponse),
                )
                token = response.result

            with logger.tracer.span('Inserting token into redis cache'):
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
