from dataclasses import dataclass, field


@dataclass
class GetOnboardingUrlRequest:
    return_url: str = field(metadata=dict(data_key='returnUrl'))


@dataclass
class GetOnboardingUrlResponse:
    success: bool
    url: str


@dataclass
class OnboardingCallbackRequest:
    sub: str
