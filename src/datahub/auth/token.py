import requests
from typing import List, Dict
from authlib.oauth2.rfc6750 import BearerTokenValidator

from datahub.settings import HYDRA_INTROSPECT_URL


class Token:

    def __init__(self, json):
        self.content = json

    @property
    def active(self) -> bool:
        return self.content['active']

    @property
    def client_id(self) -> str:
        return self.content['client_id']

    @property
    def scopes(self) -> List[str]:
        return self.content['scope'].split(' ')

    @property
    def subject(self) -> str:
        return self.content['sub']

    @property
    def issued_at(self) -> int:
        return self.content['iat']

    @property
    def expire_at(self) -> int:
        return self.content['exp']

    @property
    def issuer_url (self) -> str:
        return self.content['iss']

    @property
    def extra_data(self) -> Dict[str, str]:
        return self.content['ext']

    # required by the BearerTokenValidator
    def get_expires_at(self) -> int:
        return self.content['exp']

    # required by the BearerTokenValidator
    def get_scope(self) -> str:
        return self.content['scope']


class TokenValidator(BearerTokenValidator):
    def authenticate_token(self, token_string):
        """
        :param str token_string:
        :rtype: Token
        """
        response = requests.post(
            verify=False,
            url=HYDRA_INTROSPECT_URL,
            data={
                'token': token_string,
                'scope': ''
            },
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            },
        )

        if response.status_code == 200:
            json_response = response.json()
            if json_response.get('active') is True:
                return Token(json_response)

    def request_invalid(self, request):
        return False

    def token_revoked(self, token):
        return not token.active
