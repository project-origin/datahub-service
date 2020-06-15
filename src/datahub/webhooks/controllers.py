import marshmallow_dataclass as md

from datahub import logger
from datahub.http import Controller
from datahub.auth import Token, require_oauth, inject_token

from .service import WebhookService
from .models import SubscribeRequest, WebhookEvent


class Subscribe(Controller):
    """
    Subscribe to a webhook event.
    """
    Request = md.class_schema(SubscribeRequest)

    service = WebhookService()

    def __init__(self, event):
        """
        :param WebhookEvent event:
        """
        self.event = event

    @require_oauth('openid')
    @inject_token
    def handle_request(self, request, token):
        """
        :param SubscribeRequest request:
        :param Token token:
        :rtype: bool
        """
        self.service.subscribe(
            event=self.event,
            subject=token.subject,
            url=request.url,
            secret=request.secret,
        )

        logger.info(f'Webhook subscribed: {self.event.value}', extra={
            'subject': token.subject,
            'event': self.event.value,
            'url': request.url,
        })

        return True


class Unsubscribe(Controller):
    """
    Unsubscribe from a webhook event.
    """
    Request = md.class_schema(SubscribeRequest)

    service = WebhookService()

    def __init__(self, event):
        """
        :param WebhookEvent event:
        """
        self.event = event

    @require_oauth('openid')
    @inject_token
    def handle_request(self, request, token):
        """
        :param SubscribeRequest request:
        :param Token token:
        :rtype: bool
        """
        success = self.service.unsubscribe(
            event=self.event,
            subject=token.subject,
            url=request.url,
            secret=request.secret,
        )

        if success:
            logger.info(f'Webhook unsubscribed: {self.event.value}', extra={
                'subject': token.subject,
                'event': self.event.value,
                'url': request.url,
                'success': success,
            })

        return success
