#!/bin/sh
cd /app/migrations && pipenv run migrate
cd /app && pipenv run production
