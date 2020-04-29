import marshmallow_dataclass as md

from datahub import logger
from datahub.http import Controller
from datahub.auth import Token, require_oauth, inject_token

from .service import WebhookService
from .models import SubscribeRequest, Event


class Subscribe(Controller):
    """
    TODO
    """
    Request = md.class_schema(SubscribeRequest)

    service = WebhookService()

    def __init__(self, event):
        """
        :param Event event:
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
        )

        logger.info(f'Webhook subscription created: {self.event.value}', extra={
            'subject': token.subject,
            'event': self.event.value,
            'url': request.url,
        })

        return True
