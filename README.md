![alt text](doc/logo.png)

# Project Origin DataHubService

This is the repository for DataHubService, a part of [Project Origin](https://github.com/project-origin/documentation/).

This service is responsible for:

- Importing metering points and measurement data from ElOverblik (DataHub)
- Issuing of GGOs
- Publishing measurements and issuing GGOs on to the blockchain ledger
- Generating and storing publicly disclosed datasets
- Mapping from technology- and fuel-code to a technology label (ie. "Wind", "Solar", etc.)


## Installation and running locally

The following sections details how to install and run the project locally for development and debugging.


## Requirements

- Python 3.7
- Pip
- Pipenv
- A PostgreSQL server with one database
- A Redis server with three databases
- A Unix/Linux machine

### First time installation


Initially, make sure to define necessary environment variables (listed below).
You can define them in the .env file in the root of the project
([more details on this here](https://pipenv-fork.readthedocs.io/en/latest/advanced.html#automatic-loading-of-env)).

Start by installing a few system libraries:

    sudo apt install python3.7-dev libpq-dev

Also, make sure to upgrade your system packages for good measure:
   
    pip install --upgrade --user setuptools pip pipenv

Then install project dependencies:

    pipenv install

Then apply database migrations:

    cd src/migrations
    pipenv run migrate
    cd ../../

### Running locally (development)

This starts the local development server (NOT for production use):

    pipenv run develop

### Running tests

Run unit- and integration tests:

    pipenv run pytest


## Environment variables

Name | Description | Example
:--- | :--- | :--- |
`SERVICE_NAME` | Name of this service | `DataHubService`
`DEBUG` | Whether or not to enable debugging mode (off by default) | `0` or `1`
`SECRET` | Application secret for misc. operations | `foobar`
`CORS_ORIGINS` | Allowed CORS origins | `http://www.example.com`
**URLs:** | |
`PROJECT_URL` | Public URL to this service without trailing slash | `https://datahub.projectorigin.dk`
`ACCOUNT_SERVICE_URL` | Public URL to AccountService without trailing slash | `https://account.projectorigin.dk`
`LEDGER_URL` | URL to Blockchain Ledger without trailing slash | `https://ledger.projectorigin.dk`
`ENERGY_TYPE_SERVICE_URL` | URL to EnergyTypeService Ledger without trailing slash | `https://energytype.projectorigin.dk`
**Authentication:** | |
`HYDRA_URL` | URL to Hydra without trailing slash | `https://auth.projectorigin.dk`
`HYDRA_INTROSPECT_URL` | URL to Hydra Introspect without trailing slash | `https://authintrospect.projectorigin.dk`
**ElOverblik:** | |
`ELOVERBLIK_TOKEN` | ElOverblik (private) token | `foobar`
`ELOVERBLIK_THIRD_PARTY_ID` | ElOverblik third party ID | `spamandbacon`
`ELOVERBLIK_SERVICE_URL` | ElOverblik service URL | `https://apipreprod.eloverblik.dk/ThirdPartyApi`
`ELOVERBLIK_ONBOARDING_URL` | ElOverblik onboarding URL | `https://preprod.eloverblik.dk/Authorization/authorization`
`ELOVERBLIK_REQUEST_ACCESS_FROM` | The date to request access to data from | `2016-04-23`
`ELOVERBLIK_REQUEST_ACCESS_TO` | The date to request access to data to | `2023-04-17`
**Redis:** | |
`REDIS_HOST` | Redis hostname/IP | `127.0.0.1`
`REDIS_PORT` | Redis port number | `6379`
`REDIS_USERNAME` | Redis username | `johndoe`
`REDIS_PASSWORD` | Redis username | `qwerty`
`REDIS_CACHE_DB` | Redis database for caching (unique for this service) | `0`
`REDIS_BROKER_DB` | Redis database for task brokering (unique for this service) | `1`
`REDIS_BACKEND_DB` | Redis database for task results (unique for this service) | `2`
**Logging:** | |
`AZURE_APP_INSIGHTS_CONN_STRING` | Azure Application Insight connection string (optional) | `InstrumentationKey=19440978-19a8-4d07-9a99-b7a31d99f313`
**Database:** | |
`DATABASE_URI` | Database connection string for SQLAlchemy | `postgresql://scott:tiger@localhost/mydatabase`
`DATABASE_CONN_POLL_SIZE` | Connection pool size per container | `10`
**Runtime:** | |
`WORKERS` | Number of Gunicorn threads to run for the web API | `3`
`WORKER_CONNECTIONS` | Number of gevent greenthreads to run for each Gunicorn thread | `100`
`CONCURRENCY` | Number of gevent greenthreads to execute asynchronous tasks | `100`
**Debug:** | |
`FIRST_MEASUREMENT_TIME` | Set a date to start pulling data from | `2019-09-01T00:00:00Z`
`LAST_MEASUREMENT_TIME` | Set a date to end pulling data from | `2021-09-01T00:00:00Z`

## Building container images

Web API:

    sudo docker build -f Dockerfile.web -t datahub-service-web:v1 .

Worker:

    sudo docker build -f Dockerfile.worker -t datahub-service-worker:v1 .

Worker Beat:

    sudo docker build -f Dockerfile.beat -t datahub-service-beat:v1 .

# System architecture

The following diagram depicts the overall architecture of DataHubService and its dependencies. A few key points are listed below the diagram.

![alt text](doc/DataHubService.png)

- It exposes a web API using OAuth2 authentication.
- It has two asynchronous workers running their own processes (containers), one generic worker and one for compiling Disclosures specifically.
- The web API process starts asynchronous tasks by submitting them to a distributed queue using Redis.
- The Worker imports data from DataHub and submits it to both the SQL database and the ledger.
- A Beat process kicks off periodic tasks.


# 3rd party libraries

This project uses the following 3rd party libraries:

- [Flask](https://flask.palletsprojects.com/en/1.1.x/): HTTP framework
- [SQLAlchemy](https://www.sqlalchemy.org/): Database ORM
- [Alembic](https://alembic.sqlalchemy.org/en/latest/): Database migrations and revisioning
- [Marshmallow](https://marshmallow.readthedocs.io/en/stable/): JSON serialization/deserialization and validation
- [Celery](https://docs.celeryproject.org/): Asynchronous tasks
- [Redis](https://pypi.org/project/redis/): Celery backend + caching
- [OpenCensus](https://github.com/census-instrumentation/opencensus-python): Logging and tracing
- [Authlib](https://docs.authlib.org): OAuth2 implementation
- [Origin-Ledger-SDK](https://pypi.org/project/Origin-Ledger-SDK/): Interface with the blockchain ledger
- [bip32utils](https://github.com/lyndsysimon/bip32utils/): Generating block keys for the ledger
- [pytest](https://docs.pytest.org/): Testing
