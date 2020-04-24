from urllib.parse import urlencode

from datahub.settings import (
    ELOVERBLIK_ONBOARDING_URL,
    ELOVERBLIK_THIRD_PARTY_ID,
    ELOVERBLIK_REQUEST_ACCESS_FROM,
    ELOVERBLIK_REQUEST_ACCESS_TO,
)


#https://preprod.eloverblik.dk/Authorization/authorization?thirdPartyId=GUIIntroThirdParty01TpId&fromDate=2016-04-17&toDate=2023-04-17&customerKey=CUSTOMER-SUB&returnUrl=http%3A%2F%2Fasd.com


def generate_onboarding_url(sub, return_url):
    """
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
