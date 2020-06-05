from authlib.integrations.flask_oauth2 import (
    ResourceProtector,
    current_token,
)

from .token import TokenValidator


require_oauth = ResourceProtector()
require_oauth.register_token_validator(TokenValidator())


def inject_token(func):
    """
    Function decorator which injects a named parameter "token"
    The value is the TokenValidator provided by the client in a HTTP header.
    """
    def inject_user_wrapper(*args, **kwargs):
        kwargs['token'] = current_token
        return func(*args, **kwargs)
    return inject_user_wrapper
