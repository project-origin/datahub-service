from datetime import datetime
from dateutil.relativedelta import relativedelta

from .models import SummaryResolution


class LabelRange(object):

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
