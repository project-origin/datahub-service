#!/bin/sh
cd /app/migrations && pipenv run migrate || exit
cd /app && pipenv run production
