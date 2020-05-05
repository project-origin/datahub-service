from .ggo import Ggo
from .measurements import Measurement
from .meteringpoints import MeteringPoint
from .webhooks import Subscription
from .disclosure import (
    Disclosure,
    DisclosureMeteringPoint,
    DisclosureSettlement,
    DisclosureRetiredGgo,
)


# This is a list of all database models to include when creating
# database migrations.

VERSIONED_DB_MODELS = (
    Ggo,
    Measurement,
    MeteringPoint,
    Subscription,
    Disclosure,
    DisclosureMeteringPoint,
    DisclosureSettlement,
    DisclosureRetiredGgo,
)
