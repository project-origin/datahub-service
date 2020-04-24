# from datetime import datetime, timezone
#
# from datahub.measurements import Measurement
# from datahub.settings import GGO_EXPIRE_TIME
#
# from .models import Ggo
#
#
# class GgoIssuingController(object):
#
#     def issue_for(self, measurements, session):
#         """
#         :param collections.abc.Iterable[Measurement] measurements:
#         :param session:
#         :return:
#         """
#         for i, measurement in enumerate(measurements):
#             session.add(Ggo(
#                 issue_time=datetime.now(tz=timezone.utc),
#                 expire_time=datetime.now(tz=timezone.utc) + GGO_EXPIRE_TIME,
#                 gsrn=measurement.gsrn,
#                 address='TODO',
#                 begin=measurement.begin,
#                 end=measurement.end,
#                 amount=measurement.amount,
#             ))
