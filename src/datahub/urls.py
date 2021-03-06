from .ggo import controllers as ggo
from .auth import controllers as auth
from .technology import controllers as technology
from .measurements import controllers as measurements
from .meteringpoints import controllers as meteringpoints
from .disclosure import controllers as disclosure
from .webhooks import controllers as webhooks
from .meteringpoints import MeasurementType
from .webhooks import WebhookEvent


urls = (

    # Auth
    ('/onboarding/get-url', auth.GetOnboardingUrl()),
    ('/onboarding/callback', auth.OnboardingCallback()),

    # Technologies
    ('/technologies', technology.GetTechnologies()),

    # MeteringPoints
    ('/meteringpoints', meteringpoints.GetMeteringPoints()),
    ('/meteringpoints/set-key', meteringpoints.SetKey()),
    ('/meteringpoints/disable', meteringpoints.DisableMeteringpoints()),

    # GGOs
    ('/ggo', ggo.GetGgoList()),

    # Measurements
    ('/measurements', measurements.GetMeasurementList()),
    ('/measurements/summary', measurements.GetMeasurementSummary()),
    ('/measurements/begin-range', measurements.GetBeginRange()),
    ('/measurements/consumed', measurements.GetMeasurement(MeasurementType.CONSUMPTION)),
    ('/measurements/produced', measurements.GetMeasurement(MeasurementType.PRODUCTION)),

    # Disclosure
    ('/disclosure', disclosure.GetDisclosure()),
    ('/disclosure/list', disclosure.GetDisclosureList()),
    ('/disclosure/create', disclosure.CreateDisclosure()),
    ('/disclosure/delete', disclosure.DeleteDisclosure()),

    # Webhooks
    ('/webhook/on-measurement-published/subscribe', webhooks.Subscribe(WebhookEvent.ON_MEASUREMENT_PUBLISHED)),
    ('/webhook/on-measurement-published/unsubscribe', webhooks.Unsubscribe(WebhookEvent.ON_MEASUREMENT_PUBLISHED)),
    ('/webhook/on-ggo-issued/subscribe', webhooks.Subscribe(WebhookEvent.ON_GGOS_ISSUED)),
    ('/webhook/on-ggo-issued/unsubscribe', webhooks.Unsubscribe(WebhookEvent.ON_GGOS_ISSUED)),
    ('/webhook/on-meteringpoint-available/subscribe', webhooks.Subscribe(WebhookEvent.ON_METERINGPOINT_AVAILABLE)),
    ('/webhook/on-meteringpoint-available/unsubscribe', webhooks.Unsubscribe(WebhookEvent.ON_METERINGPOINT_AVAILABLE)),

)
