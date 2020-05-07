from celery.schedules import crontab

from datahub.tasks import celery_app

from .import_measurements import get_distinct_gsrn
from .compile_disclosure import get_disclosures


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # Executes every night at 03:00
    sender.add_periodic_task(
        crontab(hour=3, minute=0),
        get_distinct_gsrn.s(),
    )

    # Executes every night at 03:00
    sender.add_periodic_task(
        crontab(hour=3, minute=0),
        get_disclosures.s(),
    )
