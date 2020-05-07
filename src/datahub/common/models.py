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

    @property
    def delta(self):
        """
        :rtype: timedelta
        """
        return self.end - self.begin

    def with_boundaries(self, begin, end):
        """
        :param date begin:
        :param date end:
        :rtype: DateRange
        """
        return DateRange(
            begin=max(begin, min(end, self.begin)),
            end=max(begin, min(end, self.end)),
        )

    def to_datetime_range(self):
        """
        :rtype: DateTimeRange
        """
        return DateTimeRange.from_date_range(self)


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

    @property
    def delta(self):
        """
        :rtype: timedelta
        """
        return self.end - self.begin


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
