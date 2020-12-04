from datetime import timedelta


DEBUG = True


# -- Project -----------------------------------------------------------------

PROJECT_NAME = 'DataHub Service'
SERVICE_NAME = 'DataHubService'
SECRET = None
PROJECT_URL = None
CORS_ORIGINS = None
LOG_LEVEL = None


# -- Database ----------------------------------------------------------------

SQL_ALCHEMY_SETTINGS = {}
DATABASE_URI = None


# -- Services ----------------------------------------------------------------

ACCOUNT_SERVICE_URL = None
LEDGER_URL = None
ENERGY_TYPE_SERVICE_URL = None

ELOVERBLIK_TOKEN = None
ELOVERBLIK_THIRD_PARTY_ID = None
ELOVERBLIK_SERVICE_URL = None
ELOVERBLIK_ONBOARDING_URL = None
ELOVERBLIK_REQUEST_ACCESS_FROM = None
ELOVERBLIK_REQUEST_ACCESS_TO = None


# -- webhook -----------------------------------------------------------------

HMAC_HEADER = 'x-hub-signature'


# -- Auth/tokens -------------------------------------------------------------

TOKEN_HEADER = 'Authorization'

HYDRA_URL = None
HYDRA_INTROSPECT_URL = None


# -- Task broker and locking -------------------------------------------------

REDIS_HOST = None
REDIS_PORT = None
REDIS_USERNAME = None
REDIS_PASSWORD = None
REDIS_CACHE_DB = None
REDIS_BROKER_DB = None
REDIS_BACKEND_DB = None

REDIS_URL = None

REDIS_BROKER_URL = None
REDIS_BACKEND_URL = None


# -- Misc --------------------------------------------------------------------

GGO_EXPIRE_TIME = timedelta(days=90)
UNKNOWN_TECHNOLOGY_LABEL = 'Unknown'
BATCH_RESUBMIT_AFTER_HOURS = 6

AZURE_APP_INSIGHTS_CONN_STRING = None

FIRST_MEASUREMENT_TIME = None
LAST_MEASUREMENT_TIME = None
