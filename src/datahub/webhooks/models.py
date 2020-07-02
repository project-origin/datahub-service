import sqlalchemy as sa
from enum import Enum
from dataclasses import dataclass

from datahub.db import ModelBase
from datahub.ggo.models import MappedGgo
from datahub.measurements import MappedMeasurement
from datahub.meteringpoints import MappedMeteringPoint


@dataclass
class OnMeasurementPublishedRequest:
    sub: str
    measurement: MappedMeasurement


@dataclass
class OnGgoIssuedRequest:
    sub: str
    ggo: MappedGgo


@dataclass
class OnMeteringPointAvailableRequest:
    sub: str
    meteringpoint: MappedMeteringPoint


class WebhookEvent(Enum):
    ON_MEASUREMENT_PUBLISHED = 'ON_MEASUREMENT_PUBLISHED'
    ON_GGOS_ISSUED = 'ON_GGOS_ISSUED'
    ON_METERINGPOINTS_AVAILABLE = 'ON_METERINGPOINTS_AVAILABLE'
    ON_METERINGPOINT_AVAILABLE = 'ON_METERINGPOINT_AVAILABLE'


class WebhookSubscription(ModelBase):
    """
    Implementation of a single webhook event subscription.
    """
    __tablename__ = 'webhook_subscription'

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    event = sa.Column(sa.Enum(WebhookEvent), index=True, nullable=False)
    subject = sa.Column(sa.String(), index=True, nullable=False)
    url = sa.Column(sa.String(), nullable=False)
    secret = sa.Column(sa.String(), nullable=True)


# -- Subscribe request and response ------------------------------------------


@dataclass
class SubscribeRequest:
    url: str
    secret: str
