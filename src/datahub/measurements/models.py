import sqlalchemy as sa
import origin_ledger_sdk as ols
from sqlalchemy import func
from sqlalchemy.orm import relationship
from bip32utils import BIP32Key
from typing import List
from datetime import datetime
from dataclasses import dataclass, field
from marshmallow import validate

from datahub.db import ModelBase
from datahub.common import DateTimeRange, SummaryResolution, SummaryGroup
from datahub.validators import unique_values
from datahub.meteringpoints import MeasurementType


# -- Database models ---------------------------------------------------------


class Measurement(ModelBase):
    """
    TODO
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
        :rtype: str
        """
        return self.meteringpoint.sub

    @property
    def sector(self):
        """
        :rtype: str
        """
        return self.meteringpoint.sector

    @property
    def technology_code(self):
        """
        :rtype: str
        """
        return self.meteringpoint.technology_code

    @property
    def fuel_code(self):
        """
        :rtype: str
        """
        return self.meteringpoint.fuel_code

    @property
    def type(self):
        """
        :rtype: MeasurementType
        """
        return self.meteringpoint.type

    @property
    def key(self):
        """
        :rtype: BIP32Key
        """
        # Minutes since epoch
        m = int(self.begin.replace(second=0, microsecond=0).timestamp())

        return self.meteringpoint.key.ChildKey(m)

    @property
    def address(self):
        """
        :rtype: str
        """
        return ols.generate_address(ols.AddressPrefix.MEASUREMENT, self.key.PublicKey())

    @property
    def settlement_address(self):
        """
        :rtype: str
        """
        return ols.generate_address(ols.AddressPrefix.SETTLEMENT, self.key.PublicKey())

    def get_ledger_publishing_request(self):
        """
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

    def set_submitted_to_ledger(self):
        """
        Update time for when the measurement was submitted to ledger
        """
        self.submitted = func.now()


# -- Common ------------------------------------------------------------------


@dataclass
class MappedMeasurement:
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
    TODO
    """
    begin: datetime = field(default=None)
    begin_range: DateTimeRange = field(default=None, metadata=dict(data_key='beginRange'))
    sector: List[str] = field(default_factory=list)
    gsrn: List[str] = field(default_factory=list)
    type: MeasurementType = field(default=None, metadata=dict(by_value=True))


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
    filters: MeasurementFilters
    offset: int
    limit: int


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
    filters: MeasurementFilters
    fill: bool

    grouping: List[str] = field(metadata=dict(validate=(
        validate.ContainsOnly(('type', 'gsrn', 'sector')),
        unique_values,
    )))


@dataclass
class GetMeasurementSummaryResponse:
    success: bool
    labels: List[str] = field(default_factory=list)
    groups: List[SummaryGroup] = field(default_factory=list)
