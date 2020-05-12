import requests
import marshmallow_dataclass as md
from functools import partial

from datahub import logger
from datahub.cache import redis
from datahub.settings import ELOVERBLIK_SERVICE_URL, ELOVERBLIK_TOKEN, DEBUG

from .models import (
    Scope,
    Authorization,
    MeteringPoint,
    GetTokenResponse,
    GetAuthorizationsResponse,
    GetMeteringPointsResponse,
    TimeSeriesResult,
    GetTimeSeriesResponse,
)


TOKEN_EXPIRE = 3600


class EloverblikService(object):
    """
    TODO
    """
    def invoke(self, f, token, path, response_schema):
        """
        :param function f:
        :param str token:
        :param str path:
        :param Schema response_schema:
        :rtype obj:
        """
        response = f(
            url='%s%s' % (ELOVERBLIK_SERVICE_URL, path),
            headers={
                'Authorization': f'Bearer {token}',
                'Content-type': 'application/json',
                'accept': 'application/json',
            },
            verify=not DEBUG,
        )

        if response.status_code != 200:
            raise Exception('%s %d\n\n%s\n\n' % (path, response.status_code, response.content))

        response_json = response.json()

        return response_schema().load(response_json)

    def get(self, *args, **kwargs):
        return self.invoke(requests.get, *args, **kwargs)

    def post(self, body, *args, **kwargs):
        return self.invoke(partial(requests.post, json=body), *args, **kwargs)

    def get_token(self):
        """
        :rtype: str
        """
        with logger.tracer.span('ElOverblik.GetToken'):
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

    def get_authorizations(self):
        """
        :rtype: list[Authorization]
        """
        with logger.tracer.span('ElOverblik.GetAuthorizations'):
            response = self.get(
                token=self.get_token(),
                path=f'/api/Authorization/Authorizations',
                response_schema=md.class_schema(GetAuthorizationsResponse),
            )

            return response.result

    def get_meteringpoints(self, scope, identifier):
        """
        :param str token:
        :param Scope scope:
        :param str identifier:
        :rtype: list[MeteringPoint]
        """
        with logger.tracer.span('ElOverblik.GetMeteringPoints'):
            response = self.get(
                token=self.get_token(),
                path=f'/api/Authorization/Authorization/MeteringPoints/{scope.value}/{identifier}',
                response_schema=md.class_schema(GetMeteringPointsResponse),
            )

            return response.result

    def get_time_series(self, gsrn, date_from, date_to):
        """
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

        with logger.tracer.span('ElOverblik.GetTimeSeries'):
            response = self.post(
                body=body,
                token=self.get_token(),
                path=f'/api/MeterData/GetTimeSeries/{date_from_formatted}/{date_to_formatted}/Hour',
                response_schema=md.class_schema(GetTimeSeriesResponse),
            )

            return response.result
