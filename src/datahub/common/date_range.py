from datetime import datetime
from dateutil.relativedelta import relativedelta

from .models import SummaryResolution


class LabelRange(object):
    """
    Generates an ordered list of labels each corresponding to a period
    of time (defined by its begin, end, and the "resolution" parameter).

    For example, provided the following inputs::

        begin = datetime(2020, 1, 1, 0, 0, 0)
        end = datetime(2020, 1, 3, 0, 0, 0)
        resolution = SummaryResolution.day
        labels = list(LabelRange(begin, end, resolution))

    Results in the following list::

        ['2020-01-01', '2020-01-02', '2020-01-03']

    """

    RESOLUTIONS = {
        SummaryResolution.hour: '%Y-%m-%d %H:00',
        SummaryResolution.day: '%Y-%m-%d',
        SummaryResolution.month: '%Y-%m',
        SummaryResolution.year: '%Y',
    }

    LABEL_STEP = {
        SummaryResolution.hour: relativedelta(hours=1),
        SummaryResolution.day: relativedelta(days=1),
        SummaryResolution.month: relativedelta(months=1),
        SummaryResolution.year: relativedelta(years=1),
        SummaryResolution.all: None,
    }

    def __init__(self, begin, end, resolution):
        """
        :param datetime begin:
        :param datetime end:
        :param SummaryResolution resolution:
        """
        self.begin = begin
        self.end = end
        self.resolution = resolution

    def __iter__(self):
        return iter(self.get_label_range())

    def get_label_range(self):
        format = self.RESOLUTIONS[self.resolution]
        step = self.LABEL_STEP[self.resolution]
        begin = self.begin
        labels = []

        while begin < self.end:
            labels.append(begin.strftime(format))
            begin += step

        return labels
