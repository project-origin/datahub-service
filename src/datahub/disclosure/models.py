import sqlalchemy as sa
from sqlalchemy.orm import relationship
from typing import List
from datetime import date
from dataclasses import dataclass, field
from marshmallow import validate

from datahub.db import ModelBase
from datahub.common import SummaryResolution, SummaryGroup, DateRange


class Disclosure(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure'
    __table_args__ = (
        sa.UniqueConstraint('public_id'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    public_id = sa.Column(sa.String(), index=True)
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())

    sub = sa.Column(sa.String(), index=True, nullable=False)
    begin = sa.Column(sa.Date(), nullable=False)
    end = sa.Column(sa.Date(), nullable=False)

    meteringpoints = relationship('DisclosureMeteringPoint', back_populates='disclosure', uselist=True)

    def add_gsrn(self, gsrn):
        """
        :param str gsrn:
        """
        self.meteringpoints.append(DisclosureMeteringPoint(gsrn=gsrn))

    def get_gsrn(self):
        """
        :rtype list[str]:
        """
        return [mp.gsrn for mp in self.meteringpoints]


class DisclosureMeteringPoint(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure_meteringpoint'

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    disclosure_id = sa.Column(sa.Integer(), sa.ForeignKey('disclosure.id'), index=True)
    disclosure = relationship('Disclosure', foreign_keys=[disclosure_id])

    gsrn = sa.Column(sa.String(), sa.ForeignKey('meteringpoint.gsrn'), nullable=False)


class DisclosureSettlement(ModelBase):
    """
    TODO
    """
    __tablename__ = 'disclosure_settlement'

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

    id = sa.Column(sa.Integer(), primary_key=True, index=True)

    settlement_id = sa.Column(sa.String(), sa.ForeignKey('disclosure_settlement.id'), nullable=False)
    settlement = relationship('DisclosureSettlement', foreign_keys=[settlement_id])

    address = sa.Column(sa.String(), nullable=False)
    amount = sa.Column(sa.Integer(), nullable=False)
    begin = sa.Column(sa.DateTime(timezone=True), nullable=False)
    end = sa.Column(sa.DateTime(timezone=True), nullable=False)
    sector = sa.Column(sa.String(), nullable=False)
    technology_code = sa.Column(sa.String(), nullable=False)
    fuel_code = sa.Column(sa.String(), nullable=False)


# -- CreateDisclosure request and response -----------------------------------


@dataclass
class CreateDisclosureRequest:
    begin: date
    end: date
    gsrn: List[str] = field(metadata=dict(validate=validate.Length(min=1)))


@dataclass
class CreateDisclosureResponse:
    success: bool
    id: str


# -- GetDisclosure request and response --------------------------------------


@dataclass
class GetDisclosureRequest:
    id: str
    date_range: DateRange = field(metadata=dict(data_key='dateRange'))
    resolution: SummaryResolution = field(metadata=dict(by_value=True))


@dataclass
class GetDisclosureResponse:
    success: bool
    message: str = field(default=None)
    measurements: SummaryGroup = field(default=None)
    groups: List[SummaryGroup] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
