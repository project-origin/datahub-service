#!/bin/sh
cd /app/migrations/ && pipenv run alembic migrate
cd /app && pipenv run production
