import marshmallow_dataclass as md

from datahub.auth import Token, require_oauth, inject_token
from datahub.db import inject_session, atomic
from datahub.http import Controller

from .queries import MeteringPointQuery
from .models import GetMeteringPointsResponse, SetKeyRequest, SetKeyResponse
from ..pipelines import start_import_measurements_pipeline_for


class GetMeteringPoints(Controller):
    """
    TODO
    """
    Response = md.class_schema(GetMeteringPointsResponse)

    @require_oauth('meteringpoints.read')
    @inject_token
    @inject_session
    def handle_request(self, token, session):
        """
        :param Token token:
        :param Session session:
        :rtype: GetMeteringPointsResponse
        """
        meteringpoints = MeteringPointQuery(session) \
            .belongs_to(token.subject) \
            .all()

        return GetMeteringPointsResponse(
            success=True,
            meteringpoints=meteringpoints,
        )


class SetKey(Controller):
    """
    TODO
    """
    Request = md.class_schema(SetKeyRequest)
    Response = md.class_schema(SetKeyResponse)

    @require_oauth('meteringpoints.read')
    @inject_token
    def handle_request(self, request, token):
        """
        :param SetKeyRequest request:
        :param Token token:
        :rtype: SetKeyResponse
        """
        self.set_key(token.subject, request.gsrn, request.key)

        start_import_measurements_pipeline_for(request.gsrn)

        return SetKeyResponse(success=True)

    @atomic
    def set_key(self, sub, gsrn, key, session):
        """
        :param str sub:
        :param str gsrn:
        :param str key:
        :param Session session:
        """
        meteringpoint = MeteringPointQuery(session) \
            .belongs_to(sub) \
            .has_gsrn(gsrn) \
            .one()

        meteringpoint.ledger_extended_key = key
