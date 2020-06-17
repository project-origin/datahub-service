import marshmallow_dataclass as md

from datahub import logger
from datahub.auth import Token, require_oauth, inject_token
from datahub.db import inject_session, atomic
from datahub.http import Controller
from datahub.pipelines import start_import_measurements_pipeline_for

from .queries import MeteringPointQuery
from .models import GetMeteringPointsResponse, SetKeyRequest, SetKeyResponse


class GetMeteringPoints(Controller):
    """
    Returns a list of all the user's MeteringPoints.
    """
    Response = md.class_schema(GetMeteringPointsResponse)

    @require_oauth('meteringpoints.read')
    @inject_token
    @inject_session
    def handle_request(self, token, session):
        """
        :param Token token:
        :param sqlalchemy.orm.Session session:
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
    Set a BIP32 master extended key for a MeteringPoint (specified
    by its GSRN number). The key is used when publishing measurements
    and issuing GGOs for that specific MeteringPoint.

    Setting the key initiates importing Measurements for the MeteringPoint.
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

        logger.info(f'Extended key set for MeteringPoint: {request.gsrn}', extra={
            'gsrn': request.gsrn,
            'subject': token.subject,
        })

        start_import_measurements_pipeline_for(token.subject, request.gsrn)

        return SetKeyResponse(success=True)

    @atomic
    def set_key(self, sub, gsrn, key, session):
        """
        :param str sub:
        :param str gsrn:
        :param str key:
        :param sqlalchemy.orm.Session session:
        """
        meteringpoint = MeteringPointQuery(session) \
            .belongs_to(sub) \
            .has_gsrn(gsrn) \
            .one()

        meteringpoint.ledger_extended_key = key
