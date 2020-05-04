from uuid import uuid4
import marshmallow_dataclass as md

from datahub.http import Controller, BadRequest
from datahub.db import atomic, inject_session
from datahub.auth import Token, require_oauth, inject_token
from datahub.meteringpoints import MeteringPoint, MeteringPointQuery

from .queries import DisclosureRetiredGgoQuery
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
            disclosure.add_meteringpoint(
                self.get_meteringpoint(sub, gsrn, session))

        session.add(disclosure)
        session.flush()

        return disclosure

    def get_meteringpoint(self, sub, gsrn, session):
        """
        :param str sub:
        :param str gsrn:
        :param Session session:
        :rtype: MeteringPoint
        """
        meteringpoint = MeteringPointQuery(session) \
            .has_sub(sub) \
            .has_gsrn(gsrn) \
            .is_consumption() \
            .one_or_none()

        if meteringpoint is None:
            raise BadRequest((
                f'MeteringPoint with GSRN "{gsrn}" is not available, '
                'or is not eligible for disclosure'
            ))

        return meteringpoint


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
                message=f'Disclosure with ID "{request.id}" not found',
            )

        # We don't need to set the date outer boundaries to min/max at those
        # of the disclosure, as disclosure.get_measurements() does this already
        begin_range = request.date_range.to_datetime_range()

        # Get summary of the measurements which were published
        measurement_summary = disclosure \
            .get_measurements() \
            .begins_within(begin_range) \
            .get_summary(request.resolution, []) \
            .fill(begin_range)

        if measurement_summary.groups:
            measurements = measurement_summary.groups[0].values
        else:
            measurements = []

        # Get summary of the GGOs retired to the disclosed MeteringPoints
        retired_ggo_summary = DisclosureRetiredGgoQuery(session) \
            .from_disclosure(disclosure) \
            .begins_within(begin_range) \
            .get_summary(request.resolution, ['technologyCode', 'fuelCode']) \
            .fill(begin_range)

        return GetDisclosureResponse(
            success=True,
            measurements=measurements,
            ggos=retired_ggo_summary.groups,
            labels=measurement_summary.labels,
        )
