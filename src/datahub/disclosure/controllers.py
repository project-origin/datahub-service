from uuid import uuid4
import marshmallow_dataclass as md

from datahub.http import Controller, BadRequest
from datahub.db import atomic, inject_session
from datahub.auth import Token, require_oauth, inject_token
from datahub.meteringpoints import MeteringPoint, MeteringPointQuery
from datahub.pipelines import start_compile_disclosure_pipeline
from datahub.common import (
    SummaryGroup,
    LabelRange,
    SummaryResolution,
    DateTimeRange,
)

from .queries import DisclosureRetiredGgoQuery
from .models import (
    Disclosure,
    DisclosureState,
    DisclosureDataSeries,
    GetDisclosureRequest,
    GetDisclosureResponse,
    GetDisclosureListResponse,
    CreateDisclosureRequest,
    CreateDisclosureResponse,
    DeleteDisclosureRequest,
    DeleteDisclosureResponse,
)


class GetDisclosure(Controller):
    """
    Get data for a disclosure. Data is returned in the same format as for
    /measurements/summary, except this endpoint returns a list of measurements
    and GGO summaries for each disclosed meteringpoint.
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

        # Data begin and end datetimes
        if request.date_range:
            begin_range = request.date_range \
                .with_boundaries(disclosure.begin, disclosure.end) \
                .to_datetime_range()
        else:
            begin_range = disclosure.date_range.to_datetime_range()

        # Data resolution
        resolution = self.get_resolution(request, disclosure, begin_range)

        # Data
        if disclosure.publicize_meteringpoints:
            data = self.get_data_series(
                disclosure, begin_range, resolution, session)
        else:
            data = self.get_anonymized_data_series(
                disclosure, begin_range, resolution, session)

        # Data labels
        labels = list(LabelRange(
            begin_range.begin, begin_range.end, resolution))

        return GetDisclosureResponse(
            success=True,
            description=disclosure.description,
            state=disclosure.state,
            labels=labels,
            data=data,
            begin=disclosure.begin,
            end=disclosure.end,
        )

    def get_resolution(self, request, disclosure, begin_range):
        """
        :param GetDisclosureRequest request:
        :param Disclosure disclosure:
        :param DateTimeRange begin_range:
        :rtype: SummaryResolution
        """
        if request.resolution:
            resolution = request.resolution
        elif begin_range.delta.days >= (365 * 3):
            resolution = SummaryResolution.year
        elif begin_range.delta.days >= 60:
            resolution = SummaryResolution.month
        elif begin_range.delta.days >= 3:
            resolution = SummaryResolution.day
        else:
            resolution = SummaryResolution.hour

        return min(resolution, disclosure.max_resolution)

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
                address = mp.meteringpoint.physical_address

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
            measurements = measurements.has_gsrn(gsrn)

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
            ggos = ggos.has_gsrn(gsrn)

        summary = ggos \
            .get_summary(resolution, ['technology']) \
            .fill(begin_range)

        return summary.groups


class GetDisclosureList(Controller):
    """
    Returns a list of all the user's disclosures
    """
    Response = md.class_schema(GetDisclosureListResponse)

    @require_oauth('ggo.read')  # TODO disclosure
    @inject_token
    @inject_session
    def handle_request(self, token, session):
        """
        :param Token token:
        :param Session session:
        :rtype: GetDisclosureListResponse
        """
        disclosures = session.query(Disclosure) \
            .filter(Disclosure.sub == token.subject) \
            .all()

        return GetDisclosureListResponse(
            success=True,
            disclosures=disclosures,
        )


class CreateDisclosure(Controller):
    """
    Create a new, publicly available, disclosure of data for one or more
    MeteringPoints within a specific period of time. The "maxResolution"
    parameter set a limit on how high a resolution the data is available
    at when getting the disclosure. For instance, setting maxResolution=day
    will not allow getting data on an hourly basis.

    Returns the ID of the newly created disclosure.
    """
    Request = md.class_schema(CreateDisclosureRequest)
    Response = md.class_schema(CreateDisclosureResponse)

    @require_oauth('ggo.read')  # TODO disclosure
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
            name=request.name,
            description=request.description,
            max_resolution=request.max_resolution,
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
            .belongs_to(sub) \
            .has_gsrn(gsrn) \
            .is_consumption() \
            .one_or_none()

        if meteringpoint is None:
            raise BadRequest((
                f'MeteringPoint with GSRN "{gsrn}" is not available, '
                'or is not eligible for disclosure'
            ))

        return meteringpoint


class DeleteDisclosure(Controller):
    """
    Delete an existing disclosure
    """
    Request = md.class_schema(DeleteDisclosureRequest)
    Response = md.class_schema(DeleteDisclosureResponse)

    @require_oauth('ggo.read')  # TODO disclosure
    @inject_token
    @atomic
    def handle_request(self, request, token, session):
        """
        :param DeleteDisclosureRequest request:
        :param Token token:
        :param Session session:
        :rtype: DeleteDisclosureResponse
        """
        disclosure = session.query(Disclosure) \
            .filter(Disclosure.public_id == request.id) \
            .one_or_none()

        if disclosure is None:
            return DeleteDisclosureResponse(
                success=False,
                message=f'Disclosure with ID "{request.id}" not found',
            )

        session.delete(disclosure)

        return DeleteDisclosureResponse(
            success=True,
        )
