from authlib.integrations.flask_oauth2 import (
    ResourceProtector,
    current_token,
)

# from datahub.db import inject_session
# from datahub.http import Unauthorized
#
# from .models import User
# from .queries import UserQuery
from .token import TokenValidator


require_oauth = ResourceProtector()
require_oauth.register_token_validator(TokenValidator())


def inject_token(func):
    """
    Function decorator which injects a "token" named parameter
    if it doesn't already exists. The value is the authentication
    token provided by the client in a HTTP header.
    """
    def inject_user_wrapper(*args, **kwargs):
        kwargs['token'] = current_token
        return func(*args, **kwargs)
    return inject_user_wrapper


# def inject_user(required=False):
#     """
#     TODO
#
#     :param bool required:
#     """
#     def _inject_user(func):
#         """
#         Function decorator which injects a "user" named parameter
#         if it doesn't already exists. The value is a User object if
#         possible, else None.
#
#         Consumes the "token" parameter provided by previous @inject_token.
#         """
#         def inject_user_wrapper(*args, **kwargs):
#             user = _get_user()
#             if required and not user:
#                 raise Unauthorized()
#             kwargs['user'] = user
#             return func(*args, **kwargs)
#         return inject_user_wrapper
#
#     return _inject_user
#
#
# @inject_session
# def _get_user(session):
#     """
#     :param Session session:
#     :rtype: User
#     """
#     return UserQuery(session) \
#         .has_sub(current_token.subject) \
#         .one_or_none()
