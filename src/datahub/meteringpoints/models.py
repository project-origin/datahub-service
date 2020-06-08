import sqlalchemy as sa
from enum import Enum
from typing import List
from dataclasses import dataclass, field
from bip32utils import BIP32Key

from datahub.db import ModelBase


class MeasurementType(Enum):
    PRODUCTION = 'production'
    CONSUMPTION = 'consumption'


class MeteringPoint(ModelBase):
    """
    Implementation of a single MeteringPoint that belongs to a user
    identified by their subject (sub).
    """
    __tablename__ = 'meteringpoint'
    __table_args__ = (
        sa.UniqueConstraint('gsrn'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    created = sa.Column(sa.DateTime(timezone=True), server_default=sa.func.now())

    sub = sa.Column(sa.String(), index=True, nullable=False)
    gsrn = sa.Column(sa.String(), index=True, nullable=False)
    type: MeasurementType = sa.Column(sa.Enum(MeasurementType), index=True, nullable=False)
    sector = sa.Column(sa.String(), index=True, nullable=False)
    technology_code = sa.Column(sa.String())
    fuel_code = sa.Column(sa.String())

    # Physical location
    street_code = sa.Column(sa.String())
    street_name = sa.Column(sa.String())
    building_number = sa.Column(sa.String())
    city_name = sa.Column(sa.String())
    postcode = sa.Column(sa.String())
    municipality_code = sa.Column(sa.String())

    # Ledger key
    ledger_extended_key = sa.Column(sa.String())

    @property
    def key(self):
        """
        Returns the ledger key for this meteringpoint.

        :rtype: BIP32Key
        """
        return BIP32Key.fromExtendedKey(self.ledger_extended_key)

    @property
    def physical_address(self):
        """
        Returns a concatenated string representation of the physical address.

        :rtype: str
        """
        return (
            f'{self.street_name} {self.building_number}, '
            f'{self.postcode} {self.city_name}'
        )

    def is_producer(self):
        """
        Returns True if this meteringpoint is an energy producer.

        :rtype: bool
        """
        return self.type is MeasurementType.PRODUCTION

    def is_consumer(self):
        """
        Returns True if this meteringpoint is an energy consumer.

        :rtype: bool
        """
        return self.type is MeasurementType.CONSUMPTION


# -- Common ------------------------------------------------------------------


@dataclass
class MappedMeteringPoint:
    """
    A reflection of the MappedMetering class above, but supports JSON schema
    serialization/deserialization using marshmallow/marshmallow-dataclass.
    """
    gsrn: str
    type: MeasurementType = field(metadata=dict(by_value=True))
    sector: str
    technology_code: str = field(metadata=dict(data_key='technologyCode'))
    fuel_code: str = field(metadata=dict(data_key='fuelCode'))
    street_code: str = field(metadata=dict(data_key='streetCode'))
    street_name: str = field(metadata=dict(data_key='streetName'))
    building_number: str = field(metadata=dict(data_key='buildingNumber'))
    city_name: str = field(metadata=dict(data_key='cityName'))
    postcode: str = field(metadata=dict(data_key='postCode'))
    municipality_code: str = field(metadata=dict(data_key='municipalityCode'))


# -- GetMeteringPoints request and response ----------------------------------


@dataclass
class GetMeteringPointsResponse:
    success: bool
    meteringpoints: List[MappedMeteringPoint] = field(default_factory=list)


# -- SetKey request and response ---------------------------------------------


@dataclass
class SetKeyRequest:
    gsrn: str
    key: str


@dataclass
class SetKeyResponse:
    success: bool
