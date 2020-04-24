![alt text](doc/logo.png)

# Project Origin DataHub Service

TODO Describe the project here


# Environment variables

Name | Description | Example
:--- | :--- | :--- |
`DATABASE_URI` | Database connection string for SQLAlchemy | `postgresql://scott:tiger@localhost/mydatabase`
**URLs:** | |
`PROJECT_URL` | Public URL to this service without trailing slash | `https://datahub.projectorigin.dk`
`ACCOUNT_SERVICE_URL` | URL to AccountService without trailing slash | `https://account.projectorigin.dk`
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


# Building container images

Web API:

    sudo docker build -f Dockerfile.web -t datahub-service-web:v1 .

Worker:

    sudo docker build -f Dockerfile.worker -t datahub-service-worker:v1 .

Worker Beat:

    sudo docker build -f Dockerfile.beat -t datahub-service-beat:v1 .
