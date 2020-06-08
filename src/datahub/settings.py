import os
from datetime import date, datetime, timezone, timedelta


DEBUG = os.environ.get('DEBUG') in ('1', 't', 'true', 'yes')


# -- Project -----------------------------------------------------------------

PROJECT_NAME = 'DataHub Service'
SERVICE_NAME = os.environ['SERVICE_NAME']
SECRET = os.environ['SECRET']
PROJECT_URL = os.environ['PROJECT_URL']
CORS_ORIGINS = os.environ['CORS_ORIGINS']


# -- Directories/paths -------------------------------------------------------

__current_file = os.path.abspath(__file__)
__current_folder = os.path.split(__current_file)[0]

PROJECT_DIR = os.path.abspath(os.path.join(__current_folder, '..', '..'))
VAR_DIR = os.path.join(PROJECT_DIR, 'var')
SOURCE_DIR = os.path.join(PROJECT_DIR, 'src')
MIGRATIONS_DIR = os.path.join(SOURCE_DIR, 'migrations')
ALEMBIC_CONFIG_PATH = os.path.join(MIGRATIONS_DIR, 'alembic.ini')


# -- Database ----------------------------------------------------------------

SQL_ALCHEMY_SETTINGS = {
    'echo': False,
    'pool_pre_ping': True,
    'pool_size': int(os.environ['DATABASE_CONN_POLL_SIZE']),
}

DATABASE_URI = os.environ['DATABASE_URI']


# -- Services ----------------------------------------------------------------

ACCOUNT_SERVICE_URL = os.environ['ACCOUNT_SERVICE_URL']
LEDGER_URL = os.environ['LEDGER_URL']
ENERGY_TYPE_SERVICE_URL = os.environ['ENERGY_TYPE_SERVICE_URL']

ELOVERBLIK_TOKEN = os.environ['ELOVERBLIK_TOKEN']
ELOVERBLIK_THIRD_PARTY_ID = os.environ['ELOVERBLIK_THIRD_PARTY_ID']
ELOVERBLIK_SERVICE_URL = os.environ['ELOVERBLIK_SERVICE_URL']
ELOVERBLIK_ONBOARDING_URL = os.environ['ELOVERBLIK_ONBOARDING_URL']
ELOVERBLIK_REQUEST_ACCESS_FROM = date.fromisoformat(
    os.environ['ELOVERBLIK_REQUEST_ACCESS_FROM'])
ELOVERBLIK_REQUEST_ACCESS_TO = date.fromisoformat(
    os.environ['ELOVERBLIK_REQUEST_ACCESS_TO'])


# -- webhook -----------------------------------------------------------------

HMAC_HEADER = 'x-hub-signature'


# -- Auth/tokens -------------------------------------------------------------

TOKEN_HEADER = 'Authorization'

HYDRA_URL = os.environ['HYDRA_URL']
HYDRA_INTROSPECT_URL = os.environ['HYDRA_INTROSPECT_URL']


# -- Task broker and locking -------------------------------------------------

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
REDIS_USERNAME = os.environ['REDIS_USERNAME']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
REDIS_CACHE_DB = int(os.environ['REDIS_CACHE_DB'])
REDIS_BROKER_DB = int(os.environ['REDIS_BROKER_DB'])
REDIS_BACKEND_DB = int(os.environ['REDIS_BACKEND_DB'])

REDIS_URL = 'redis://%s:%s@%s:%d' % (
    REDIS_USERNAME, REDIS_PASSWORD, REDIS_HOST, REDIS_PORT)

REDIS_BROKER_URL = '%s/%d' % (REDIS_URL, REDIS_BROKER_DB)
REDIS_BACKEND_URL = '%s/%d' % (REDIS_URL, REDIS_BACKEND_DB)


# -- Misc --------------------------------------------------------------------

GGO_EXPIRE_TIME = timedelta(days=90)
UNKNOWN_TECHNOLOGY_LABEL = 'Unknown'
BATCH_RESUBMIT_AFTER_HOURS = 6

AZURE_APP_INSIGHTS_CONN_STRING = os.environ.get(
    'AZURE_APP_INSIGHTS_CONN_STRING')

# Used when debugging for importing test data
if os.environ.get('FIRST_MEASUREMENT_TIME'):
    FIRST_MEASUREMENT_TIME = datetime\
        .strptime(os.environ['FIRST_MEASUREMENT_TIME'], '%Y-%m-%dT%H:%M:%SZ') \
        .astimezone(timezone.utc)
else:
    FIRST_MEASUREMENT_TIME = None
