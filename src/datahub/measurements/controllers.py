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
    Measurement)


class GetMeasurement(Controller):
    """
    Returns a single Measurement object of a specific type,
    either PRODUCTION or CONSUMPTION.
    """
    Request = md.class_schema(GetMeasurementRequest)
    Response = md.class_schema(GetMeasurementResponse)

    def __init__(self, typ):
        """
        :param MeasurementType typ:
        """
        self.typ = typ

    @require_oauth('measurements.read')
    @inject_token
    @inject_session
    def handle_request(self, request, token, session):
        """
        :param GetMeasurementRequest request:
        :param Token token:
        :param sqlalchemy.orm.Session session:
        :rtype: GetMeasurementResponse
        """
        measurement = MeasurementQuery(session) \
            .is_active() \
            .belongs_to(token.subject) \
            .is_published() \
            .is_type(self.typ) \
            .has_gsrn(request.gsrn) \
            .begins_at(request.begin) \
            .one_or_none()

        return GetMeasurementResponse(
            success=measurement is not None,
            measurement=measurement,
        )


class GetMeasurementList(Controller):
    """
    Returns a list of Measurement objects which belongs to any of the
    user's MeteringPoints with options to filter/narrow down the results.
    Can only select MeteringPoints which belongs to the user.
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
        :param sqlalchemy.orm.Session session:
        :rtype: GetMeasurementListResponse
        """
        query = MeasurementQuery(session) \
            .is_active() \
            .belongs_to(token.subject) \
            .is_published()

        if request.filters:
            query = query.apply_filters(request.filters)

        results = query \
            .order_by(self.get_order_by(request)) \
            .offset(request.offset)

        if request.limit:
            results = results.limit(request.limit)

        return GetMeasurementListResponse(
            success=True,
            total=query.count(),
            measurements=results.all(),
        )

    def get_order_by(self, request):
        """
        :param GetMeasurementListRequest request:
        """
        if request.order == 'begin':
            field = Measurement.begin
        elif request.order == 'amount':
            field = Measurement.amount
        else:
            raise RuntimeError('Should NOT have happened')

        if request.sort == 'asc':
            return field.asc()
        elif request.sort == 'desc':
            return field.desc()
        else:
            raise RuntimeError('Should NOT have happened')


class GetBeginRange(Controller):
    """
    Given a set of filters, this endpoint returns the first and
    last "begin" for all measurements in the result set.
    Useful for checking when measuring began and ended.
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
        :param sqlalchemy.orm.Session session:
        :rtype: GetBeginRangeResponse
        """
        query = MeasurementQuery(session) \
            .is_active() \
            .belongs_to(token.subject) \
            .is_published()

        if request.filters:
            query = query.apply_filters(request.filters)

        return GetBeginRangeResponse(
            success=True,
            first=query.get_first_measured_begin(),
            last=query.get_last_measured_begin(),
        )


class GetMeasurementSummary(Controller):
    """
    Returns a summary of the user's Measurements, or a subset hereof.
    Useful for plotting or visualizing data, or wherever aggregated
    data is needed.
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
        :param sqlalchemy.orm.Session session:
        :rtype: GetMeasurementSummaryResponse
        """
        query = MeasurementQuery(session) \
            .is_active() \
            .belongs_to(token.subject) \
            .is_published()

        if request.filters:
            query = query.apply_filters(request.filters)

        summary = query.get_summary(
            request.resolution, request.grouping, request.utc_offset)

        if request.fill and request.filters.begin_range:
            summary.fill(request.filters.begin_range)

        return GetMeasurementSummaryResponse(
            success=True,
            labels=summary.labels,
            groups=summary.groups,
        )
