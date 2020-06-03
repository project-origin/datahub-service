from celery.schedules import crontab

from datahub.tasks import celery_app

from .import_measurements import get_distinct_gsrn
from .resubmit_measurements import resubmit_measurements
from .compile_disclosure import get_disclosures


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # IMPORT MEASUREMENTS FROM ELOVERBLIK
    # Executes every 3rd hour (at 03, 06, 09, etc.)
    sender.add_periodic_task(
        crontab(hour='2,5,8.11,14,17,20,23', minute=0),
        get_distinct_gsrn.s(),
    )

    # RESUBMIT UNPUBLISHED MEASUREMENTS TO THE LEDGER
    # Executes every hour
    sender.add_periodic_task(
        crontab(hour='*/1', minute=0),
        resubmit_measurements.s(),
    )

    # COMPILE DISCLOSURES
    # Executes every night at 03:00
    sender.add_periodic_task(
        crontab(hour=3, minute=0),
        get_disclosures.s(),
    )
