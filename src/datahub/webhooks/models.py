import sqlalchemy as sa
from enum import Enum
from dataclasses import dataclass

from datahub.db import ModelBase


class Event(Enum):
    ON_GGOS_ISSUED = 'ON_GGOS_ISSUED'
    ON_METERINGPOINTS_AVAILABLE = 'ON_METERINGPOINTS_AVAILABLE'


class Subscription(ModelBase):
    """
    Implementation of a single webhook event subscription.
    """
    __tablename__ = 'webhook_subscription'

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    event = sa.Column(sa.Enum(Event), index=True, nullable=False)
    subject = sa.Column(sa.String(), index=True, nullable=False)
    url = sa.Column(sa.String(), nullable=False)
    secret = sa.Column(sa.String(), nullable=True)


# -- Subscribe request and response ------------------------------------------


@dataclass
class SubscribeRequest:
    url: str
    secret: str
