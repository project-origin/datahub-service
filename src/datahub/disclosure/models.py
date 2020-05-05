import sqlalchemy as sa
from enum import Enum
from sqlalchemy.orm import relationship
from typing import List
from datetime import date
from dataclasses import dataclass, field
from marshmallow import validate, fields
from marshmallow_dataclass import NewType

from datahub.db import ModelBase, Session
from datahub.measurements import MeasurementQuery
from datahub.common import SummaryResolution, SummaryGroup, DateRange
from datahub.meteringpoints import MeteringPoint


class DisclosureState(Enum):
    PENDING = 'PENDING'
    PROCESSING = 'PROCESSING'
    AVAILABLE = 'AVAILABLE'


class Disclosure(ModelBase):
    """
    banner_status = postgresql.ENUM('PENDING', 'PROCESSING', 'AVAILABLE', name='disclosurestate')
    banner_status.create(op.get_bind())
    op.add_column('disclosure', sa.Column('state', sa.Enum('PENDING', 'PROCESSING', 'AVAILABLE', name='disclosurestate'), nullable=False))
    """
    __tablename__ = 'disclosure'
    __table_args__ = (
        sa.UniqueConstraint('public_id'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    public_id = sa.Column(sa.String(), index=True)
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())
    state = sa.Column(sa.Enum(DisclosureState), nullable=False)

    sub = sa.Column(sa.String(), index=True, nullable=False)
    begin = sa.Column(sa.Date(), nullable=False)
    end = sa.Column(sa.Date(), nullable=False)

    publicize_meteringpoints = sa.Column(sa.Boolean(), nullable=False)
    publicize_gsrn = sa.Column(sa.Boolean(), nullable=False)
    publicize_physical_address = sa.Column(sa.Boolean(), nullable=False)

    meteringpoints = relationship('DisclosureMeteringPoint', back_populates='disclosure', uselist=True)

    @property
    def date_range(self):
        """
        :rtype: DateRange
        """
        return DateRange(begin=self.begin, end=self.end)

    def add_meteringpoint(self, meteringpoint):
        """
        :param MeteringPoint meteringpoint:
        """
        self.meteringpoints.append(
            DisclosureMeteringPoint(gsrn=meteringpoint.gsrn))

    def get_gsrn(self):
        """
        :rtype list[str]:
        """
        return [mp.gsrn for mp in self.meteringpoints]

    def get_measurements(self):
        """
        :rtype: MeasurementQuery
        """
        return MeasurementQuery(Session.object_session(self)) \
            .has_any_gsrn(self.get_gsrn()) \
            .begins_within(self.date_range.to_datetime_range())


class DisclosureMeteringPoint(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure_meteringpoint'
    __table_args__ = (
        sa.UniqueConstraint('disclosure_id', 'gsrn'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    disclosure_id = sa.Column(sa.Integer(), sa.ForeignKey('disclosure.id'), index=True)
    disclosure = relationship('Disclosure', foreign_keys=[disclosure_id])

    gsrn = sa.Column(sa.String(), sa.ForeignKey('meteringpoint.gsrn'), nullable=False)
    meteringpoint = relationship('MeteringPoint', foreign_keys=[gsrn])


class DisclosureSettlement(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure_settlement'
    __table_args__ = (
        sa.UniqueConstraint('disclosure_id', 'measurement_id'),
        sa.UniqueConstraint('disclosure_id', 'address'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    disclosure_id = sa.Column(sa.Integer(), sa.ForeignKey('disclosure.id'), index=True)
    disclosure = relationship('Disclosure', foreign_keys=[disclosure_id])

    measurement_id = sa.Column(sa.Integer(), sa.ForeignKey('measurement.id'), index=True)
    measurement = relationship('Measurement', foreign_keys=[measurement_id])

    ggos = relationship('DisclosureRetiredGgo', back_populates='settlement', uselist=True)

    address = sa.Column(sa.String(), nullable=False)


class DisclosureRetiredGgo(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure_ggo'
    __table_args__ = (
        sa.UniqueConstraint('settlement_id', 'address'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    settlement_id = sa.Column(sa.Integer(), sa.ForeignKey('disclosure_settlement.id'), nullable=False)
    settlement = relationship('DisclosureSettlement', foreign_keys=[settlement_id])

    address = sa.Column(sa.String(), nullable=False)
    amount = sa.Column(sa.Integer(), nullable=False)
    begin = sa.Column(sa.DateTime(timezone=True), nullable=False)
    end = sa.Column(sa.DateTime(timezone=True), nullable=False)
    sector = sa.Column(sa.String(), nullable=False)
    technology_code = sa.Column(sa.String(), nullable=False)
    fuel_code = sa.Column(sa.String(), nullable=False)


# -- Common ------------------------------------------------------------------


@dataclass
class MappedDisclosure:
    public_id: str = field(metadata=dict(data_key='id'))
    begin: date
    end: date
    publicize_meteringpoints: bool = field(metadata=dict(data_key='publicizeMeteringpoints'))
    publicize_gsrn: bool = field(metadata=dict(data_key='publicizeGsrn'))
    publicize_physical_address: bool = field(metadata=dict(data_key='publicizePhysicalAddress'))


# -- GetDisclosure request and response --------------------------------------


@dataclass
class DisclosureDataSeries:
    gsrn: str = field(default=None)
    address: str = field(default=None)
    measurements: List[int] = field(default_factory=list)
    ggos: List[SummaryGroup] = field(default_factory=list)



@dataclass
class GetDisclosureRequest:
    id: str
    date_range: DateRange = field(metadata=dict(data_key='dateRange'))
    resolution: SummaryResolution = field(metadata=dict(by_value=True))


@dataclass
class GetDisclosureResponse:
    success: bool
    message: str = field(default=None)
    state: DisclosureState = field(default=None, metadata=dict(by_value=True))
    labels: List[str] = field(default_factory=list)
    data: List[DisclosureDataSeries] = field(default_factory=list)


# -- GetDisclosureList request and response ----------------------------------


@dataclass
class GetDisclosureListResponse:
    success: bool
    disclosures: List[MappedDisclosure] = field(default_factory=list)


# -- CreateDisclosure request and response -----------------------------------


@dataclass
class CreateDisclosureRequest:
    begin: date
    end: date
    publicize_meteringpoints: bool = field(metadata=dict(data_key='publicizeMeteringpoints'))
    publicize_gsrn: bool = field(metadata=dict(data_key='publicizeGsrn'))
    publicize_physical_address: bool = field(metadata=dict(data_key='publicizePhysicalAddress'))
    gsrn: List[str] = field(metadata=dict(validate=validate.Length(min=1)))


@dataclass
class CreateDisclosureResponse:
    success: bool
    id: str


# -- DeleteDisclosure request and response -----------------------------------


@dataclass
class DeleteDisclosureRequest:
    id: str


@dataclass
class DeleteDisclosureResponse:
    success: bool
    message: str = field(default=None)
