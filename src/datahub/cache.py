from redis import Redis

from .settings import (
    REDIS_USERNAME,
    REDIS_PASSWORD,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_CACHE_DB,
)

redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    db=REDIS_CACHE_DB,
)
