from uuid import uuid4
import marshmallow_dataclass as md

from datahub.http import Controller, BadRequest
from datahub.db import atomic, inject_session
from datahub.auth import Token, require_oauth, inject_token
from datahub.meteringpoints import MeteringPoint, MeteringPointQuery
from datahub.pipelines import start_compile_disclosure_pipeline
from datahub.common import SummaryGroup, LabelRange

from .queries import DisclosureRetiredGgoQuery
from .models import (
    Disclosure,
    DisclosureState,
    DisclosureDataSeries,
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

        start_compile_disclosure_pipeline(new_disclosure)

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
            state=DisclosureState.PENDING,
            sub=sub,
            begin=request.begin,
            end=request.end,
            publicize_meteringpoints=request.publicize_meteringpoints,
            publicize_gsrn=request.publicize_gsrn,
            publicize_physical_address=request.publicize_physical_address,
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

        # Set the date outer boundaries to those of the disclosure
        begin_range = request.date_range \
            .with_boundaries(disclosure.begin, disclosure.end) \
            .to_datetime_range()

        if disclosure.publicize_meteringpoints:
            data = self.get_data_series(
                disclosure, begin_range, request.resolution, session)
        else:
            data = self.get_anonymized_data_series(
                disclosure, begin_range, request.resolution, session)

        labels = list(LabelRange(
            begin_range.begin, begin_range.end, request.resolution))

        return GetDisclosureResponse(
            success=True,
            state=disclosure.state,
            labels=labels,
            data=data,
        )

    def get_data_series(self, disclosure, begin_range, resolution, session):
        """
        :rtype: list[DisclosureDataSeries]
        """
        data = []

        for mp in disclosure.meteringpoints:
            gsrn = None
            address = None

            if disclosure.publicize_gsrn:
                gsrn = mp.gsrn
            if disclosure.publicize_physical_address:
                address = mp.meteringpoint.address

            data.append(DisclosureDataSeries(
                gsrn=gsrn,
                address=address,
                measurements=self.get_measurements(disclosure, begin_range, resolution, mp.gsrn),
                ggos=self.get_ggos(disclosure, begin_range, resolution, session, mp.gsrn),
            ))

        return data

    def get_anonymized_data_series(self, disclosure, begin_range, resolution, session):
        """
        :rtype: list[DisclosureDataSeries]
        """
        return [
            DisclosureDataSeries(
                measurements=self.get_measurements(disclosure, begin_range, resolution),
                ggos=self.get_ggos(disclosure, begin_range, resolution, session),
            )
        ]

    def get_measurements(self, disclosure, begin_range, resolution, gsrn=None):
        """
        :rtype: list[int]
        """
        measurements = disclosure \
            .get_measurements() \
            .begins_within(begin_range)

        if gsrn:
            measurements.has_gsrn(gsrn)

        summary = measurements \
            .get_summary(resolution, []) \
            .fill(begin_range)

        if summary.groups:
            return summary.groups[0].values
        else:
            return []

    def get_ggos(self, disclosure, begin_range, resolution, session, gsrn=None):
        """
        :rtype: list[SummaryGroup]
        """
        ggos = DisclosureRetiredGgoQuery(session) \
            .from_disclosure(disclosure) \
            .begins_within(begin_range)

        if gsrn:
            ggos.has_gsrn(gsrn)

        summary = ggos \
            .get_summary(resolution, ['technologyCode', 'fuelCode']) \
            .fill(begin_range)

        return summary.groups
