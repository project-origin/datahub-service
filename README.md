![alt text](doc/logo.png)

# Project Origin example application backend

Project Origin API/backend

TODO Describe the project briefly here.


# Contents

- Installing and running the project
    - Requirements
    - First time installation
    - Running locally (development)
    - Running tests
- Project configuration
    - Environment variables
    - Project settings
- System design and implementation
    - Dependencies
- Database (models and migrations)
    - Workflow
    - Make migrations
    - Apply migrations
- Testing
- Glossary


# Installing and running the project

The following sections describes how to install and run the project locally for development and debugging.

### Requirements

- Python 3.7
- Pip
- Pipenv

### First time installation

Make sure to upgrade your system packages for good measure:
   
    pip install --upgrade --user setuptools pip pipenv

Then install project dependencies:

    pipenv install
   
Then apply database migrations (while at the same time creating an empty SQLite database-file, if using SQLite):

    cd src/migrations
    pipenv run alembic upgrade head
    cd ../../

Then (optionally) seed the database with some data:

    pipenv run python src/seed.py

### Running locally (development)

This starts the local development server (NOT for production use):

    pipenv run python src/serve.py

### Running tests

Run unit- and integration tests:

    pipenv run pytest


# Project configuration

### Environment variables

TODO describe necessary environment variables

### Project settings

TODO describe src/origin/settings.py


# System design and implementation

### Dependencies

- SQLAlchemy
- marshmellow
- TODO


# Database (models and migrations)

TODO

## Workflow

TODO

## Make migrations

TODO

## Apply migrations

TODO


# Domain knowledge

TODO Alot to describe here...

## Terminology

TODO

## GGO calculation

- Stored = Issued + Inbound - Outbound - Expired
- Retired = Stored - Consumed


# Testing

Testing is done in a number of different ways, including:

- Pure unit-testing using mocked dependencies
- Unit-testing using a SQLite database, where assertions are made on the data stored in the database after the test
- Complex multi-component integration- and functional testing using both mocked dependencies and a SQLite database

*NOTE: Because of the use of SQLite in testing its necessary for the process executing
the tests to have write-access to the /var folder.*


# Glossary

TODO Describe commonly used terms
