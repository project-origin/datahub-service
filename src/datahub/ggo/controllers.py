import marshmallow_dataclass as md

from datahub.auth import Token, require_oauth, inject_token
from datahub.db import inject_session
from datahub.http import Controller

from .queries import GgoQuery
from .models import GetGgoListRequest, GetGgoListResponse


class GetGgoList(Controller):
    """
    Returns a list of GGO objects that have been issued to
    a MeteringPoint identified by the provided GSRN number.
    Can only select MeteringPoints which belongs to the user.

    "begin" is the time at which the energy production began.
    It usually have an end time which is one hour later,
    but only the begin is filtered upon. It is possible to
    filters GGOs on a range/period defined by a from- and to datetime.
    """
    Request = md.class_schema(GetGgoListRequest)
    Response = md.class_schema(GetGgoListResponse)

    @require_oauth('ggo.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetGgoListRequest request:
        :param Token token:
        :param Session session:
        :rtype: GetGgoListResponse
        """
        ggos = GgoQuery(session) \
            .belongs_to(token.subject) \
            .is_published() \
            .has_gsrn(request.gsrn) \
            .begins_within(request.begin_range) \
            .all()

        return GetGgoListResponse(
            success=True,
            ggos=ggos,
        )
