import json
import hmac
import requests
import marshmallow_dataclass as md
from hashlib import sha256
from base64 import b64encode
from datetime import datetime
from dataclasses import dataclass

from datahub import logger
from datahub.settings import DEBUG, HMAC_HEADER
from datahub.db import atomic, inject_session

from .models import Subscription, Event


@dataclass
class OnGgoIssuedRequest:
    sub: str
    gsrn: str
    begin: datetime


@dataclass
class OnMeteringointsAvailableRequest:
    sub: str


class WebhookService(object):

    @atomic
    def subscribe(self, event, subject, url, secret, session):
        """
        :param Event event:
        :param str subject:
        :param str url:
        :param str secret:
        :param Session session:
        """
        session.add(Subscription(
            event=event,
            subject=subject,
            url=url,
            secret=secret,
        ))

    @inject_session
    def publish(self, event, subject, schema, request, session):
        """
        :param Event event:
        :param str subject:
        :param Schema schema:
        :param obj request:
        :param Session session:
        """
        filters = (
            Subscription.event == event,
            Subscription.subject == subject,
        )

        subscriptions = session.query(Subscription) \
            .filter(*filters) \
            .all()

        for subscription in subscriptions:
            body = schema().dump(request)

            hmac_header = 'sha256=' + b64encode(hmac.new(
                subscription.secret.encode(),
                json.dumps(body).encode(),
                sha256
            ).digest()).decode()

            headers = {
                HMAC_HEADER: hmac_header
            }

            logger.info(f'Invoking webhook: {event.value}', extra={
                'subject': subject,
                'event': event.value,
                'url': subscription.url,
                'request': str(body),
            })

            try:
                response = requests.post(
                    url=subscription.url,
                    json=body,
                    headers=headers,
                    verify=not DEBUG,
                )
            except:
                logger.exception(f'Failed to invoke webhook: {event.value}', extra={
                    'subject': subject,
                    'event': event.value,
                    'url': subscription.url,
                    'request': str(body),
                })
                continue

            if response.status_code != 200:
                logger.error('Invoking webhook resulted in status code != 200', extra={
                    'subject': subject,
                    'event': event.value,
                    'url': subscription.url,
                    'request': str(body),
                    'response_status_code': response.status_code,
                    'response_body': response.content.decode(),
                })

    def on_ggo_issued(self, subject, gsrn, begin):
        """
        :param str subject:
        :param str gsrn:
        :param datetime begin:
        """
        return self.publish(
            event=Event.ON_GGOS_ISSUED,
            subject=subject,
            schema=md.class_schema(OnGgoIssuedRequest),
            request=OnGgoIssuedRequest(
                sub=subject,
                gsrn=gsrn,
                begin=begin,
            )
        )

    def on_meteringpoints_available(self, subject):
        """
        :param str subject:
        """
        return self.publish(
            event=Event.ON_METERINGPOINTS_AVAILABLE,
            subject=subject,
            schema=md.class_schema(OnMeteringointsAvailableRequest),
            request=OnMeteringointsAvailableRequest(
                sub=subject,
            )
        )
