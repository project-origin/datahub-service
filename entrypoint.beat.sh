#!/bin/sh

# Run database migrations, or exit if failing
cd /app/migrations && pipenv run migrate || exit

# Make sure to "exec" before the command to forward SIGTERM to the child process
cd /app && exec pipenv run celery beat -A datahub.pipelines -l info
