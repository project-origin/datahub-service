import marshmallow_dataclass as md

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
    Generates and returns an absolute URL to perform ElOverblik onboarding.
    The generated URL is personal to the user (subject) identified by
    their access token.

    Requests must provide a return URL, where the client is redirected to
    after completing the onboarding process. The client is, however,
    redirected back to DataHubService (OnboardingCallback, below)
    before being redirected to the provided return URL.
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

        self.start_import_meteringpoints_delayed(token.subject)

        return GetOnboardingUrlResponse(
            success=True,
            url=onboarding_url,
        )

    def start_import_meteringpoints_delayed(self, subject):
        """
        Starts a number of tasks to import MeteringPoints for the subject
        requesting the onboarding URL. In case the user (client) does not
        return to the OnboardingCallback endpoint (below), then MeteringPoints
        are never imported, so this implements a sort of insurance/fallback.

        These tasks are very cheap to execute, and MeteringPoints are never
        imported twice, so it's safe to do this over and over again.
        """
        start_import_meteringpoints_pipeline(
            subject, countdown=10*60, IS_DELAYED=True)
        start_import_meteringpoints_pipeline(
            subject, countdown=30*60, IS_DELAYED=True)
        start_import_meteringpoints_pipeline(
            subject, countdown=1*60*60, IS_DELAYED=True)
        start_import_meteringpoints_pipeline(
            subject, countdown=2*60*60, IS_DELAYED=True)
        start_import_meteringpoints_pipeline(
            subject, countdown=6*60*60, IS_DELAYED=True)


class OnboardingCallback(Controller):
    """
    Callback endpoint to handle completion of ElOverblik onboarding process.
    Upon completion is triggered a pipeline to import their MeteringPoints.
    """
    METHOD = 'GET'

    Request = md.class_schema(OnboardingCallbackRequest)

    def handle_request(self, request):
        """
        :param OnboardingCallbackRequest request:
        :rtype: flask.Response
        """
        redis_key = 'onboarding-return-url-%s' % request.sub
        return_url = redis.get(redis_key)

        start_import_meteringpoints_pipeline(request.sub)

        if return_url:
            redis.delete(redis_key)
            return redirect(return_url, 303)
