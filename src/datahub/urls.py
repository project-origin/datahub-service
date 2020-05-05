from .ggo import controllers as ggo
from .auth import controllers as auth
from .measurements import controllers as measurements
from .meteringpoints import controllers as meteringpoints
from .disclosure import controllers as disclosure
from .webhooks import controllers as webhooks

from .meteringpoints import MeasurementType
from .webhooks import Event


urls = (

    # -- Auth ----------------------------------------------------------------

    ('/onboarding/get-url', auth.GetOnboardingUrl()),
    ('/onboarding/callback', auth.OnboardingCallback()),


    # -- GGOs ----------------------------------------------------------------

    ('/ggo', ggo.GetGgoList()),


    # -- MeteringPoints ------------------------------------------------------

    ('/meteringpoints', meteringpoints.GetMeteringPoints()),
    ('/meteringpoints/set-key', meteringpoints.SetKey()),


    # -- Measurements --------------------------------------------------------

    ('/measurements', measurements.GetMeasurementList()),
    ('/measurements/summary', measurements.GetMeasurementSummary()),
    ('/measurements/begin-range', measurements.GetBeginRange()),
    ('/measurements/consumed', measurements.GetMeasurement(
        MeasurementType.CONSUMPTION)),
    ('/measurements/produced', measurements.GetMeasurement(
        MeasurementType.PRODUCTION)),


    # -- Disclosure ----------------------------------------------------------

    ('/disclosure', disclosure.GetDisclosure()),
    ('/disclosure/create', disclosure.CreateDisclosure()),


    # -- Webhooks ------------------------------------------------------------

    ('/webhook/on-meteringpoints-available/subscribe', webhooks.Subscribe(
        Event.ON_METERINGPOINTS_AVAILABLE)),
    ('/webhook/on-ggos-issued/subscribe', webhooks.Subscribe(
        Event.ON_GGOS_ISSUED)),

)
