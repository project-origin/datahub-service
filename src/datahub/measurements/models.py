import sqlalchemy as sa
import origin_ledger_sdk as ols
from sqlalchemy.orm import relationship
from bip32utils import BIP32Key
from enum import Enum
from typing import List
from datetime import datetime
from dataclasses import dataclass, field
from marshmallow import validate

from datahub.db import ModelBase
from datahub.common import DateTimeRange
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
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())

    gsrn = sa.Column(sa.String(), sa.ForeignKey('meteringpoint.gsrn'), index=True, nullable=False)
    begin: datetime = sa.Column(sa.DateTime(timezone=True), index=True, nullable=False)
    end: datetime = sa.Column(sa.DateTime(timezone=True), nullable=False)
    amount = sa.Column(sa.Integer(), nullable=False)

    meteringpoint = relationship('MeteringPoint', foreign_keys=[gsrn], lazy='joined')
    ggo = relationship('Ggo', back_populates='measurement', uselist=False)

    def __str__(self):
        return 'Measurement<gsrn=%s, begin=%s, amount=%d>' % (
            self.gsrn, self.begin, self.amount)

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
    begin_range: DateTimeRange = field(default_factory=None, metadata=dict(data_key='beginRange'))
    sector: List[str] = field(default_factory=list)
    gsrn: List[str] = field(default_factory=list)
    type: MeasurementType = field(default=None, metadata=dict(by_value=True))


class SummaryResolution(Enum):
    """
    TODO
    """
    ALL = 'all'
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    HOUR = 'hour'


@dataclass
class SummaryGroup:
    """
    TODO
    """
    group: List[str] = field(default_factory=list)
    values: List[int] = field(default_factory=list)


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
    resolution: SummaryResolution = field(metadata=dict(by_value=True))
    filters: MeasurementFilters
    fill: bool

    grouping: List[str] = field(metadata=dict(validate=(
        validate.ContainsOnly(('type', 'gsrn', 'sector')),
        unique_values,
    )))

    type: MeasurementType = field(default=None, metadata=dict(by_value=True))


@dataclass
class GetMeasurementSummaryResponse:
    success: bool
    labels: List[str] = field(default_factory=list)
    groups: List[SummaryGroup] = field(default_factory=list)
