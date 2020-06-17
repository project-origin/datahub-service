from urllib.parse import urlencode

from datahub.settings import (
    ELOVERBLIK_ONBOARDING_URL,
    ELOVERBLIK_THIRD_PARTY_ID,
    ELOVERBLIK_REQUEST_ACCESS_FROM,
    ELOVERBLIK_REQUEST_ACCESS_TO,
)


def generate_onboarding_url(sub, return_url):
    """
    Generate an absolute URL to perform onboarding on ElOverblik. The URL
    is unique for a single user (subject).

    :param str sub:
    :param str return_url:
    :rtype: str
    """
    query_string = {
        'thirdPartyId': ELOVERBLIK_THIRD_PARTY_ID,
        'fromDate': ELOVERBLIK_REQUEST_ACCESS_FROM.strftime('%Y-%m-%d'),
        'toDate': ELOVERBLIK_REQUEST_ACCESS_TO.strftime('%Y-%m-%d'),
        'customerKey': sub,
        'returnUrl': return_url,
    }

    return '%s?%s' % (
        ELOVERBLIK_ONBOARDING_URL,
        urlencode(query_string),
    )
