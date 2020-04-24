#!/bin/sh
cd /app/migrations/ && pipenv run alembic upgrade head
cd /app && pipenv run gunicorn -b 0.0.0.0:8000 datahub:app -k gevent
