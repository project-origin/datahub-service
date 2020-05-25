"""
Resubmits unpublished measurements to the ledger. These are the measurements
which has not been successfully submitted to the ledger for some reason,
for instance if the ledger has been down for a period of time etc.
"""
from celery import group

from datahub import logger
from datahub.db import inject_session
from datahub.tasks import celery_app
from datahub.measurements import MeasurementQuery

from .import_measurements import submit_to_ledger


def start_resubmit_measurements_pipeline():
    """
    TODO
    """
    resubmit_measurements \
        .s() \
        .apply_async()


@celery_app.task(
    name='resubmit_measurements.resubmit_measurements',
    queue='import-measurements',
    autoretry_for=(Exception,),
    retry_backoff=2,
    max_retries=5,
)
@logger.wrap_task(
    title='Resubmitting unpublished measurements',
    pipeline='resubmit_measurements',
    task='resubmit_measurements',
)
@inject_session
def resubmit_measurements(session):
    """
    :param Session session:
    """
    measurements = MeasurementQuery(session) \
        .needs_resubmit_to_ledger() \
        .all()

    tasks = [
        submit_to_ledger.s(subject=m.sub, measurement_id=m.id)
        for m in measurements
    ]

    if measurements:
        group(tasks).apply_async()
