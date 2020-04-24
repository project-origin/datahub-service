from enum import Enum
from datetime import date, datetime, timedelta
from dataclasses import dataclass, field
from typing import List

from marshmallow import validates_schema, ValidationError


class Unit(Enum):
    Wh = 1
    KWh = 10**3
    MWh = 10**6
    GWh = 10**9


@dataclass
class DataSet:
    label: str
    values: List[int] = field(default_factory=list)
    unit: Unit = Unit.Wh


@dataclass
class DateRange:
    begin: date
    end: date

    @validates_schema
    def validate_begin_before_end(self, data, **kwargs):
        if data['begin'] >= data['end']:
            raise ValidationError({
                'begin': ['Must be before end'],
                'end': ['Must be after begin'],
            })


@dataclass
class DateTimeRange:
    begin: datetime
    end: datetime

    @validates_schema
    def validate_begin_before_end(self, data, **kwargs):
        if data['begin'] >= data['end']:
            raise ValidationError({
                'begin': ['Must be before end'],
                'end': ['Must be after begin'],
            })

    @classmethod
    def from_date_range(cls, date_range):
        """
        :param DateRange date_range:
        :rtype: DateTimeRange
        """
        return DateTimeRange(
            begin=datetime.fromordinal(date_range.begin.toordinal()),
            end=datetime.fromordinal(date_range.end.toordinal()) + timedelta(days=1),
        )
