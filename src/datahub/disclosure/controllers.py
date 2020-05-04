from uuid import uuid4
import marshmallow_dataclass as md

from datahub.http import Controller
from datahub.common import DateTimeRange
from datahub.db import atomic, inject_session
from datahub.auth import Token, require_oauth, inject_token
from datahub.measurements import MeasurementQuery

from .models import (
    Disclosure,
    CreateDisclosureRequest,
    CreateDisclosureResponse,
    GetDisclosureRequest,
    GetDisclosureResponse,
)


class CreateDisclosure(Controller):
    """
    TODO
    """
    Request = md.class_schema(CreateDisclosureRequest)
    Response = md.class_schema(CreateDisclosureResponse)

    @require_oauth('ggo.read')
    @inject_token
    def handle_request(self, request, token):
        """
        :param CreateDisclosureRequest request:
        :param Token token:
        :rtype: CreateDisclosureResponse
        """
        new_disclosure = self.create_disclosure(
            request, token.subject)

        return CreateDisclosureResponse(
            success=True,
            id=new_disclosure.public_id,
        )

    @atomic
    def create_disclosure(self, request, sub, session):
        """
        :param CreateDisclosureRequest request:
        :param str sub:
        :param Session session:
        :rtype: Disclosure
        """
        disclosure = Disclosure(
            public_id=str(uuid4()),
            sub=sub,
            begin=request.begin,
            end=request.end,
        )

        for gsrn in request.gsrn:
            disclosure.add_gsrn(gsrn)

        session.add(disclosure)
        session.flush()

        return disclosure

import origin_ledger_sdk

class GetDisclosure(Controller):
    """
    TODO
    """
    Request = md.class_schema(GetDisclosureRequest)
    Response = md.class_schema(GetDisclosureResponse)

    @inject_session
    def handle_request(self, request, session):
        """
        :param GetDisclosureRequest request:
        :param Session session:
        :rtype: GetDisclosureResponse
        """
        disclosure = session.query(Disclosure) \
            .filter(Disclosure.public_id == request.id) \
            .one_or_none()

        if disclosure is None:
            return GetDisclosureResponse(
                success=False,
                message=f'Disclosure by ID {request.id} not found',
            )

        begin_range = DateTimeRange.from_date_range(request.date_range)

        measurement_summary = MeasurementQuery(session) \
            .has_any_gsrn(disclosure.get_gsrn()) \
            .begins_within(begin_range) \
            .get_summary(request.resolution, []) \
            .fill(begin_range)

        return GetDisclosureResponse(
            success=True,
            measurements=measurement_summary.groups,
            labels=measurement_summary.labels,
        )
