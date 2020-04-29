from celery import Celery
from celery.exceptions import Retry

from datahub.settings import REDIS_BROKER_URL, REDIS_BACKEND_URL


celery_app = Celery(
    main='tasks',
    broker=REDIS_BROKER_URL,
    backend=REDIS_BACKEND_URL,
)
