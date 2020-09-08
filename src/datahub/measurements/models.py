import sqlalchemy as sa
import origin_ledger_sdk as ols
from sqlalchemy import func
from sqlalchemy.orm import relationship
from bip32utils import BIP32Key
from typing import List
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from marshmallow import validate, validates_schema, ValidationError, post_load

from datahub.db import ModelBase
from datahub.common import DateTimeRange, SummaryResolution, SummaryGroup
from datahub.validators import unique_values
from datahub.meteringpoints import MeasurementType


# -- Database models ---------------------------------------------------------


class Measurement(ModelBase):
    """
    Implementation of a single measurement that has been measured
    by a MeteringPoint. It consists of a period of time (defined by its
    begin) along with the amount of energy produced or consumed
    in Wh (Watts per hour). The type of measurement (production or
    consumption) depends on the MeteringPoint. Only one measurement can
    exists per MeteringPoint per begin.

    Measurements of type PRODUCTION also has a GGO issued to it.

    NOTE REGARDING PUBLISHING:
    Measurements (and their respective GGOs) are not publicly available
    until they have been published to the ledger (the "published" property).
    """
    __tablename__ = 'measurement'
    __table_args__ = (
        sa.UniqueConstraint('gsrn', 'begin'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    # Time when measurement was imported from ElOverblik / inserted to DB
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())

    # Time when measurement was LAST submitted to ledger (if at all)
    submitted = sa.Column(sa.DateTime(timezone=True), nullable=True)

    gsrn = sa.Column(sa.String(), sa.ForeignKey('meteringpoint.gsrn'), index=True, nullable=False)
    begin: datetime = sa.Column(sa.DateTime(timezone=True), index=True, nullable=False)
    end: datetime = sa.Column(sa.DateTime(timezone=True), nullable=False)
    amount = sa.Column(sa.Integer(), nullable=False)

    meteringpoint = relationship('MeteringPoint', foreign_keys=[gsrn], lazy='joined')
    ggo = relationship('Ggo', back_populates='measurement', uselist=False)

    # Whether or not the measurement (plus is GGO if production)
    # is published to the ledger
    published = sa.Column(sa.Boolean(), nullable=False)

    def __str__(self):
        return 'Measurement<gsrn=%s, begin=%s, amount=%d>' % (
            self.gsrn, self.begin, self.amount)

    @property
    def sub(self):
        """
        Returns the subject who owns the MeteringPoint which this
        measurement belongs to (ie. user ID).

        :rtype: str
        """
        return self.meteringpoint.sub

    @property
    def sector(self):
        """
        Returns the sector (Price area) of the MeteringPoint which this
        measurement belongs to.

        :rtype: str
        """
        return self.meteringpoint.sector

    @property
    def technology_code(self):
        """
        Returns the technology code of the MeteringPoint which this
        measurement belongs to.

        :rtype: str
        """
        return self.meteringpoint.technology_code

    @property
    def fuel_code(self):
        """
        Returns the fuel code of the MeteringPoint which this
        measurement belongs to.

        :rtype: str
        """
        return self.meteringpoint.fuel_code

    @property
    def type(self):
        """
        Returns the type og measurement, ie. PRODUCTION or CONSUMPTION.

        :rtype: MeasurementType
        """
        return self.meteringpoint.type

    @property
    def key(self):
        """
        Returns the ledger key for this measurement.

        :rtype: BIP32Key
        """
        # Minutes since epoch
        m = int(self.begin.replace(second=0, microsecond=0).timestamp())

        return self.meteringpoint.key.ChildKey(m)

    @property
    def address(self):
        """
        Returns the (unique) address this measurement has on the ledger.

        :rtype: str
        """
        return ols.generate_address(
            ols.AddressPrefix.MEASUREMENT, self.key.PublicKey())

    @property
    def settlement_address(self):
        """
        Returns the (unique) address on the ledger to put settlements when
        retiring GGOs to this measurement.

        :rtype: str
        """
        return ols.generate_address(
            ols.AddressPrefix.SETTLEMENT, self.key.PublicKey())

    def get_ledger_publishing_request(self):
        """
        Returns the publishing request object used to publish the
        GGO to the ledger.

        :rtype: PublishMeasurementRequest
        """
        if self.meteringpoint.type is MeasurementType.PRODUCTION:
            typ = ols.MeasurementType.PRODUCTION
        elif self.meteringpoint.type is MeasurementType.CONSUMPTION:
            typ = ols.MeasurementType.CONSUMPTION
        else:
            raise RuntimeError('Should NOT have happened!')

        return ols.PublishMeasurementRequest(
            address=self.address,
            begin=self.begin,
            end=self.end,
            sector=self.sector,
            type=typ,
            amount=self.amount,
        )

    def build_batch(self):
        """
        Builds and returns a ledger Batch to publish the measurement,
        and issue the GGO (if any exists).

        :rtype: ols.Batch
        """
        batch = ols.Batch(self.meteringpoint.key.PrivateKey())
        batch.add_request(self.get_ledger_publishing_request())
        if self.ggo:
            batch.add_request(self.ggo.get_ledger_issuing_request())
        return batch

    def set_submitted_to_ledger(self):
        """
        Update time for when the measurement was submitted to ledger
        """
        self.submitted = func.now()


# -- Common ------------------------------------------------------------------


@dataclass
class MappedMeasurement:
    """
    A reflection of the Measurement class above, but supports JSON schema
    serialization/deserialization using marshmallow/marshmallow-dataclass.
    """
    address: str
    begin: datetime
    end: datetime
    amount: int
    gsrn: str
    sector: str
    type: MeasurementType = field(metadata=dict(by_value=True))


@dataclass
class MeasurementFilters:
    """
    Filters to filter on Measurements when using MeasurementQuery.
    """
    begin: datetime = field(default=None)
    begin_range: DateTimeRange = field(default=None, metadata=dict(data_key='beginRange'))
    sector: List[str] = field(default_factory=list)
    gsrn: List[str] = field(default_factory=list)
    type: MeasurementType = field(default=None, metadata=dict(by_value=True))

    @validates_schema
    def validate_begin_and_begin_range_mutually_exclusive(self, data, **kwargs):
        if data.get('begin') and data.get('begin_range'):
            raise ValidationError({
                'begin': ['Field is mutually exclusive with beginRange'],
                'beginRange': ['Field is mutually exclusive with begin'],
            })


# -- GetMeasurement request and response -------------------------------------


@dataclass
class GetMeasurementRequest:
    gsrn: str
    begin: datetime


@dataclass
class GetMeasurementResponse:
    success: bool
    measurement: MappedMeasurement


# -- GetMeasurementList request and response ---------------------------------


@dataclass
class GetMeasurementListRequest:

    # Offset from UTC in hours
    utc_offset: int = field(metadata=dict(required=False, missing=0, data_key='utcOffset'))

    filters: MeasurementFilters = field(default=None)
    offset: int = field(default=0)
    limit: int = field(default=None)
    order: str = field(default='begin', metadata=dict(validate=validate.OneOf(['begin', 'amount'])))
    sort: str = field(default='asc', metadata=dict(validate=validate.OneOf(['asc', 'desc'])))

    @post_load
    def apply_time_offset(self, data, **kwargs):
        """
        Applies the request utcOffset to filters.begin and filters.begin_range
        if they don't already have a UTC offset applied to them by the client.
        """
        tzinfo = timezone(timedelta(hours=data['utc_offset']))

        if data['filters'].begin and data['filters'].begin.utcoffset() is None:
            data['filters'].begin = \
                data['filters'].begin.replace(tzinfo=tzinfo)

        if data['filters'].begin_range:
            if data['filters'].begin_range.begin.utcoffset() is None:
                data['filters'].begin_range.begin = \
                    data['filters'].begin_range.begin.replace(tzinfo=tzinfo)

            if data['filters'].begin_range.end.utcoffset() is None:
                data['filters'].begin_range.end = \
                    data['filters'].begin_range.end.replace(tzinfo=tzinfo)

        return data


@dataclass
class GetMeasurementListResponse:
    success: bool
    total: int
    measurements: List[MappedMeasurement] = field(default_factory=list)


# -- GetBeginRange request and response --------------------------------------


@dataclass
class GetBeginRangeRequest:
    filters: MeasurementFilters = field(default=None)


@dataclass
class GetBeginRangeResponse:
    success: bool
    first: datetime
    last: datetime


# -- GetGgoSummary request and response --------------------------------------


@dataclass
class GetMeasurementSummaryRequest:
    resolution: SummaryResolution
    fill: bool

    grouping: List[str] = field(metadata=dict(validate=(
        validate.ContainsOnly(('type', 'gsrn', 'sector')),
        unique_values,
    )))

    # Offset from UTC in hours
    utc_offset: int = field(metadata=dict(required=False, missing=0, data_key='utcOffset'))

    filters: MeasurementFilters = field(default=None)

    @post_load
    def apply_time_offset(self, data, **kwargs):
        """
        Applies the request utcOffset to filters.begin and filters.begin_range
        if they don't already have a UTC offset applied to them by the client.
        """
        tzinfo = timezone(timedelta(hours=data['utc_offset']))

        if data['filters'].begin and data['filters'].begin.utcoffset() is None:
            data['filters'].begin = \
                data['filters'].begin.replace(tzinfo=tzinfo)

        if data['filters'].begin_range:
            if data['filters'].begin_range.begin.utcoffset() is None:
                data['filters'].begin_range.begin = \
                    data['filters'].begin_range.begin.replace(tzinfo=tzinfo)

            if data['filters'].begin_range.end.utcoffset() is None:
                data['filters'].begin_range.end = \
                    data['filters'].begin_range.end.replace(tzinfo=tzinfo)

        return data


@dataclass
class GetMeasurementSummaryResponse:
    success: bool
    labels: List[str] = field(default_factory=list)
    groups: List[SummaryGroup] = field(default_factory=list)
