import marshmallow_dataclass as md

from datahub.db import atomic, inject_session
from datahub.cache import redis
from datahub.settings import PROJECT_URL
from datahub.http import Controller, redirect
from datahub.services.eloverblik import generate_onboarding_url
from datahub.pipelines import start_import_meteringpoints_pipeline

from .token import Token
from .decorators import require_oauth, inject_token
from .models import (
    GetOnboardingUrlRequest,
    GetOnboardingUrlResponse,
    OnboardingCallbackRequest,
)


class GetOnboardingUrl(Controller):
    """
    TODO
    """
    Request = md.class_schema(GetOnboardingUrlRequest)
    Response = md.class_schema(GetOnboardingUrlResponse)

    @require_oauth('openid')
    @inject_token
    def handle_request(self, request, token):
        """
        :param OnboardingRequest request:
        :param Token token:
        :rtype: GetOnboardingUrlResponse
        """
        redis_key = 'onboarding-return-url-%s' % token.subject
        redis.set(redis_key, request.return_url)

        onboarding_return_url = '%s/onboarding/callback?sub=%s' % (
            PROJECT_URL, token.subject)

        onboarding_url = generate_onboarding_url(
            token.subject, onboarding_return_url)

        return GetOnboardingUrlResponse(
            success=True,
            url=onboarding_url,
        )


class OnboardingCallback(Controller):
    """
    TODO
    """
    METHOD = 'GET'

    Request = md.class_schema(OnboardingCallbackRequest)

    @inject_session
    def handle_request(self, request, session):
        """
        :param OnboardingCallbackRequest request:
        :param Session session:
        :rtype: flask.Response
        """
        redis_key = 'onboarding-return-url-%s' % request.sub
        return_url = redis.get(redis_key)

        if return_url:
            start_import_meteringpoints_pipeline(request.sub)

            redis.delete(redis_key)

            return redirect(return_url, 303)
