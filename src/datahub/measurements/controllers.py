import marshmallow_dataclass as md

from datahub.auth import Token, require_oauth, inject_token
from datahub.db import inject_session
from datahub.http import Controller
from datahub.meteringpoints import MeasurementType

from .queries import MeasurementQuery
from .models import (
    GetMeasurementResponse,
    GetMeasurementRequest,
    GetMeasurementListRequest,
    GetMeasurementListResponse,
    GetBeginRangeRequest,
    GetBeginRangeResponse,
    GetMeasurementSummaryRequest,
    GetMeasurementSummaryResponse,
)


class GetMeasurement(Controller):
    """
    Get a single measurement of a specific type (PRODUCTION or CONSUMPTION).
    """
    Request = md.class_schema(GetMeasurementRequest)
    Response = md.class_schema(GetMeasurementResponse)

    def __init__(self, type):
        """
        :param MeasurementType type:
        """
        self.type = type

    @require_oauth('measurements.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetMeasurementRequest request:
        :param Token token:
        :param Session session:
        :rtype: GetMeasurementResponse
        """
        measurement = MeasurementQuery(session) \
            .belongs_to(token.subject) \
            .is_type(self.type) \
            .has_gsrn(request.gsrn) \
            .begins_at(request.begin) \
            .one_or_none()

        return GetMeasurementResponse(
            success=measurement is not None,
            measurement=measurement,
        )


class GetMeasurementList(Controller):
    """
    Get a list of measurements.
    """
    Request = md.class_schema(GetMeasurementListRequest)
    Response = md.class_schema(GetMeasurementListResponse)

    @require_oauth('measurements.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetMeasurementListRequest request:
        :param Token token:
        :param Session session:
        :rtype: GetMeasurementListResponse
        """
        query = MeasurementQuery(session) \
            .belongs_to(token.subject) \
            .apply_filters(request.filters)

        measurements = query \
            .offset(request.offset) \
            .limit(request.limit) \
            .all()

        total = query.count()

        return GetMeasurementListResponse(
            success=True,
            total=total,
            measurements=measurements,
        )


class GetBeginRange(Controller):
    """
    Get the first and last "begin" for a series of measurements.
    """
    Request = md.class_schema(GetBeginRangeRequest)
    Response = md.class_schema(GetBeginRangeResponse)

    @require_oauth('measurements.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetBeginRangeRequest request:
        :param Token token:
        :param Session session:
        :rtype: GetBeginRangeResponse
        """
        query = MeasurementQuery(session) \
            .belongs_to(token.subject)

        if request.filters:
            query = query.apply_filters(request.filters)

        return GetBeginRangeResponse(
            success=True,
            first=query.get_first_measured_begin(),
            last=query.get_last_measured_begin(),
        )


class GetMeasurementSummary(Controller):
    """
    Get an aggregated summary of measurements.
    """
    Request = md.class_schema(GetMeasurementSummaryRequest)
    Response = md.class_schema(GetMeasurementSummaryResponse)

    @require_oauth('measurements.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetMeasurementSummaryRequest request:
        :param Token token:
        :param Session session:
        :rtype: GetMeasurementSummaryResponse
        """
        summary = MeasurementQuery(session) \
            .belongs_to(token.subject) \
            .apply_filters(request.filters) \
            .get_summary(request.resolution, request.grouping)

        if request.fill and request.filters.begin_range:
            summary.fill(request.filters.begin_range)

        return GetMeasurementSummaryResponse(
            success=True,
            labels=summary.labels,
            groups=summary.groups,
        )
