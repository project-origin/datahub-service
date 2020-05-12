#!/bin/sh
cd /app/migrations && pipenv run migrate || exit
cd /app && pipenv run celery worker -A datahub.pipelines -O fair -l info --pool=gevent --concurrency=$CONCURRENCY --queues import-measurements,import-meteringpoints
