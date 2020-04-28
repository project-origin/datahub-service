import requests
import marshmallow_dataclass as md
from datetime import datetime
from dataclasses import dataclass, field

from datahub.db import atomic, inject_session

from .models import Subscription, Event
from ..settings import DEBUG

@dataclass
class OnGgoIssuedRequest:
    sub: str
    gsrn: str
    begin_from: datetime = field(metadata=dict(data_key='beginFrom'))
    begin_to: datetime = field(metadata=dict(data_key='beginTo'))


@dataclass
class OnMeteringointsAvailableRequest:
    sub: str


class WebhookService(object):

    @atomic
    def subscribe(self, event, subject, url, session):
        """
        :param Event event:
        :param str subject:
        :param str url:
        :param Session session:
        """
        session.add(Subscription(
            event=event,
            subject=subject,
            url=url,
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

            try:
                response = requests.post(subscription.url, json=body, verify=not DEBUG)
            except:
                # TODO logging
                raise
                continue

            if response.status_code != 200:
                raise Exception('%s\n\n%s\n\n' % (body, response.content))

    def on_ggo_issued(self, subject, gsrn, begin_from, begin_to):
        """
        :param str subject:
        :param str gsrn:
        :param datetime begin_from:
        :param datetime begin_to:
        """
        return self.publish(
            event=Event.ON_GGOS_ISSUED,
            subject=subject,
            schema=md.class_schema(OnGgoIssuedRequest),
            request=OnGgoIssuedRequest(
                sub=subject,
                gsrn=gsrn,
                begin_from=begin_from,
                begin_to=begin_to,
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
