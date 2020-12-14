#!/bin/sh

# Run database migrations, or exit if failing
cd /app/migrations && pipenv run migrate || exit

# Make sure to "exec" before the command to forward SIGTERM to the child process
cd /app && exec pipenv run gunicorn -b 0.0.0.0:8089 datahub:app --workers $WORKERS --worker-class gevent --worker-connections $WORKER_CONNECTIONS --timeout 300
