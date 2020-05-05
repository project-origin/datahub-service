from datetime import datetime
from dateutil.relativedelta import relativedelta

from .models import SummaryResolution


class LabelRange(object):

    RESOLUTIONS = {
        SummaryResolution.HOUR: '%Y-%m-%d %H:00',
        SummaryResolution.DAY: '%Y-%m-%d',
        SummaryResolution.MONTH: '%Y-%m',
        SummaryResolution.YEAR: '%Y',
    }

    LABEL_STEP = {
        SummaryResolution.HOUR: relativedelta(hours=1),
        SummaryResolution.DAY: relativedelta(days=1),
        SummaryResolution.MONTH: relativedelta(months=1),
        SummaryResolution.YEAR: relativedelta(years=1),
        SummaryResolution.ALL: None,
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
