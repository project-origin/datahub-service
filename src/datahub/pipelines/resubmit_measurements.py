"""
Asynchronous tasks for re-submitting Measurements to the ledger.

One entrypoint exists:

    start_resubmit_measurements_pipeline()

"""
from datahub import logger
from datahub.db import inject_session
from datahub.tasks import celery_app
from datahub.measurements import MeasurementQuery

from .import_measurements import start_submit_measurement_pipeline


def start_resubmit_measurements_pipeline():
    """
    Starts a pipeline which resubmits unpublished measurements to the ledger.
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
    title='Getting unpublished measurements (to be re-submitted)',
    pipeline='resubmit_measurements',
    task='resubmit_measurements',
)
@inject_session
def resubmit_measurements(session):
    """
    Resubmits unpublished measurements to the ledger. These are the
    measurements which has not been successfully submitted to the ledger
    for some reason, for instance if the ledger has been down for a
    period of time etc.

    :param sqlalchemy.orm.Session session:
    """
    measurements = MeasurementQuery(session) \
        .needs_resubmit_to_ledger() \
        .all()

    for measurement in measurements:
        start_submit_measurement_pipeline(
            measurement, measurement.meteringpoint, session)
