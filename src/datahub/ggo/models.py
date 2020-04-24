import sqlalchemy as sa
import origin_ledger_sdk as ols
from sqlalchemy.orm import relationship
from bip32utils import BIP32Key
from typing import List
from datetime import datetime
from dataclasses import dataclass, field

from datahub.db import ModelBase
from datahub.common import DateTimeRange


# -- Database models ---------------------------------------------------------


class Ggo(ModelBase):
    """
    TODO
    """
    __tablename__ = 'ggo'
    __table_args__ = (
        sa.UniqueConstraint('measurement_id'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())

    issue_time = sa.Column(sa.DateTime(timezone=True), nullable=False)
    expire_time = sa.Column(sa.DateTime(timezone=True), nullable=False)

    measurement_id = sa.Column(sa.Integer(), sa.ForeignKey('measurement.id'), index=True)
    measurement = relationship('Measurement', foreign_keys=[measurement_id], lazy='joined', back_populates='ggo')

    @property
    def meteringpoint(self):
        """
        :rtype: Meteringpoints
        """
        return self.measurement.meteringpoint

    @property
    def gsrn(self):
        """
        :rtype: str
        """
        return self.meteringpoint.gsrn

    @property
    def sector(self):
        """
        :rtype: str
        """
        return self.meteringpoint.sector

    @property
    def begin(self):
        """
        :rtype: datetime
        """
        return self.measurement.begin

    @property
    def end(self):
        """
        :rtype: datetime
        """
        return self.measurement.end

    @property
    def amount(self):
        """
        :rtype: int
        """
        return self.measurement.amount

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
    def key(self):
        """
        :rtype: BIP32Key
        """
        return self.measurement.key

    @property
    def address(self):
        """
        :rtype: str
        """
        return ols.generate_address(ols.AddressPrefix.GGO, self.key.PublicKey())

    def get_ledger_issuing_request(self):
        """
        :rtype: IssueGGORequest
        """
        return ols.IssueGGORequest(
            measurement_address=self.measurement.address,
            ggo_address=self.address,
            tech_type=self.meteringpoint.technology_code,
            fuel_type=self.meteringpoint.fuel_code,
        )


# -- Common ------------------------------------------------------------------


@dataclass
class MappedGgo:
    address: str
    begin: datetime
    end: datetime
    amount: int
    gsrn: str
    sector: str
    issue_time: str = field(metadata=dict(data_key='issueTime'))
    expire_time: str = field(metadata=dict(data_key='expireTime'))
    technology_code: str = field(metadata=dict(data_key='technologyCode'))
    fuel_code: str = field(metadata=dict(data_key='fuelCode'))


# -- GetGgoList request and response -----------------------------------------


@dataclass
class GetGgoListRequest:
    gsrn: str
    begin_range: DateTimeRange = field(metadata=dict(data_key='beginRange'))


@dataclass
class GetGgoListResponse:
    success: bool
    ggos: List[MappedGgo] = field(default_factory=list)
