from enum import Enum
from typing import List
from datetime import datetime
from dataclasses import dataclass, field
from marshmallow import EXCLUDE
from decimal import Decimal


class Scope(Enum):
    AuthorizationID = 'authorizationId'
    CustomerCVR = 'customerCVR'
    CustomerKey = 'customerKey'


class MeterPointType(Enum):
    CONSUMPTION = 'E17'
    PRODUCTION = 'E18'


@dataclass
class ChildMeteringPoint:
    metering_point_id: str = field(default=None, metadata=dict(data_key='meteringPointId'))
    parent_metering_point_id: str = field(default=None, metadata=dict(data_key='parentMeteringPointId'))
    type_of_mp: str = field(default=None, metadata=dict(data_key='typeOfMP'))
    meter_reading_occurrence: str = field(default=None, metadata=dict(data_key='meterReadingOccurrence'))
    meter_number: str = field(default=None, metadata=dict(data_key='meterNumber'))


@dataclass
class MeteringPoint:
    metering_point_id: str = field(default=None, metadata=dict(data_key='meteringPointId'))
    type_of_mp: MeterPointType = field(default=None, metadata=dict(data_key='typeOfMP', by_value=True))
    access_from: str = field(default=None, metadata=dict(data_key='accessFrom'))
    access_to: str = field(default=None, metadata=dict(data_key='accessTo'))
    street_code: str = field(default=None, metadata=dict(data_key='streetCode'))
    street_name: str = field(default=None, metadata=dict(data_key='streetName'))
    building_number: str = field(default=None, metadata=dict(data_key='buildingNumber'))
    floor_id: str = field(default=None, metadata=dict(data_key='floorId'))
    room_id: str = field(default=None, metadata=dict(data_key='roomId'))
    postcode: str = field(default=None, metadata=dict(data_key='postcode'))
    city_name: str = field(default=None, metadata=dict(data_key='cityName'))
    city_sub_division_name: str = field(default=None, metadata=dict(data_key='citySubDivisionName'))
    municipality_code: str = field(default=None, metadata=dict(data_key='municipalityCode'))
    location_description: str = field(default=None, metadata=dict(data_key='locationDescription'))
    settlement_method: str = field(default=None, metadata=dict(data_key='settlementMethod'))
    meter_reading_occurrence: str = field(default=None, metadata=dict(data_key='meterReadingOccurrence'))
    first_consumer_party_name: str = field(default=None, metadata=dict(data_key='firstConsumerPartyName'))
    second_consumer_party_name: str = field(default=None, metadata=dict(data_key='secondConsumerPartyName'))
    consumer_cvr: str = field(default=None, metadata=dict(data_key='consumerCVR'))
    data_access_cvr: str = field(default=None, metadata=dict(data_key='dataAccessCVR'))
    meter_number: str = field(default=None, metadata=dict(data_key='meterNumber'))
    consumer_start_date: str = field(default=None, metadata=dict(data_key='consumerStartDate'))
    child_metering_points: List[ChildMeteringPoint] = field(default_factory=list, metadata=dict(data_key='childMeteringPoints'))

    @property
    def gsrn(self):
        return self.metering_point_id


@dataclass
class GetTokenResponse:
    result: str


@dataclass
class GetMeteringPointsResponse:
    result: List[MeteringPoint]


# -- TimeSeries response data ------------------------------------------------


class TimeSeriesUnit(Enum):
    KWH = 10**3
    MWH = 10**3


@dataclass
class TimeSeriesInterval:
    class Meta:
        unknown = EXCLUDE

    start: datetime
    end: datetime


@dataclass
class TimeSeriesPoint:
    class Meta:
        unknown = EXCLUDE

    position: int
    quantity: Decimal = field(metadata=dict(data_key='out_Quantity.quantity'))


@dataclass
class TimeSeriesPeriod:
    class Meta:
        unknown = EXCLUDE

    time_interval: TimeSeriesInterval = field(metadata=dict(data_key='timeInterval'))
    point: List[TimeSeriesPoint] = field(metadata=dict(data_key='Point'))
    resolution: str


@dataclass
class TimeSeries:
    class Meta:
        unknown = EXCLUDE

    mrid: str = field(metadata=dict(data_key='mRID'))
    unit: TimeSeriesUnit = field(metadata=dict(data_key='measurement_Unit.name'))
    period: List[TimeSeriesPeriod] = field(metadata=dict(data_key='Period'))


@dataclass
class TimeSeriesDocument:
    class Meta:
        unknown = EXCLUDE

    time_series: List[TimeSeries] = field(metadata=dict(data_key='TimeSeries'))


@dataclass
class TimeSeriesResult:
    class Meta:
        unknown = EXCLUDE

    document: TimeSeriesDocument = field(metadata=dict(data_key='MyEnergyData_MarketDocument', allow_none=True))


@dataclass
class GetTimeSeriesResponse:
    result: List[TimeSeriesResult]
