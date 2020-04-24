import os
from datetime import date, datetime, timezone, timedelta


DEBUG = os.environ.get('DEBUG') in ('1', 't', 'true', 'yes')

# -- Project -----------------------------------------------------------------

PROJECT_NAME = 'DataHub Service'

PROJECT_URL = os.environ.get(
    'PROJECT_URL', 'http://127.0.0.1:8089')


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
    'echo': DEBUG and 0,
    'pool_pre_ping': True,
    'pool_size': 20,
}
SQLITE_PATH = os.path.join(VAR_DIR, 'db.sqlite')
TEST_SQLITE_PATH = os.path.join(VAR_DIR, 'db.test.sqlite')
# DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///%s' % SQLITE_PATH)
DATABASE_URI = os.environ.get('DATABASE_URI', 'postgresql://postgres:1234@172.17.0.2/datahub')

USING_POSTGRES = DATABASE_URI.startswith('postgresql://')
USING_SQLITE = DATABASE_URI.startswith('sqlite://')


# -- Services ----------------------------------------------------------------

ELOVERBLIK_TOKEN = os.environ.get(
    'ELOVERBLIK_TOKEN', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJUSElSRFBBUlRZQVBJX1JlZnJlc2giLCJ0b2tlbmlkIjoiYTk0YTlhMDMtN2ZhMy00YzJjLTljMGItNTkzYTVjMjFjZWFmIiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiJHVUlJbnRyb1RoaXJkUGFydHkwMSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IkZvcm5hdm4iLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9zdXJuYW1lIjoiRWZ0ZXJuYXZuIiwianRpIjoiYTk0YTlhMDMtN2ZhMy00YzJjLTljMGItNTkzYTVjMjFjZWFmIiwiY3ZyIjoiMjcwNzE0MDEiLCJ0cGlkIjoiR1VJSW50cm9UaGlyZFBhcnR5MDFUcElkIiwibG9naW5UeXBlIjoiS2V5Q2FyZCIsImV4cCI6MTYxODM4NjkzMiwiaXNzIjoiRW5lcmdpbmV0IiwidG9rZW5OYW1lIjoidGVzdDQyIiwicm9sZXMiOiJSZWFkUHJpdmF0ZSwgUmVhZEJ1c2luZXNzIiwiYXVkIjoiRW5lcmdpbmV0In0.UUy227mMtCS-tMEo7ROeavuKSMeCP9N9d0YniGSodK8')

ELOVERBLIK_THIRD_PARTY_ID = os.environ.get(
    'ELOVERBLIK_THIRD_PARTY_ID', 'GUIIntroThirdParty01TpId')

ELOVERBLIK_REQUEST_ACCESS_FROM = date.fromisoformat(os.environ.get(
    'ELOVERBLIK_REQUEST_ACCESS_FROM', '2016-04-23'))

ELOVERBLIK_REQUEST_ACCESS_TO = date.fromisoformat(os.environ.get(
    'ELOVERBLIK_REQUEST_ACCESS_TO', '2023-04-17'))

ELOVERBLIK_SERVICE_URL = os.environ.get(
    'ELOVERBLIK_SERVICE_URL', 'https://apipreprod.eloverblik.dk/ThirdPartyApi')

ELOVERBLIK_ONBOARDING_URL = os.environ.get(
    'ELOVERBLIK_ONBOARDING_URL', 'https://preprod.eloverblik.dk/Authorization/authorization')

ACCOUNT_SERVICE_URL = os.environ.get(
    'ACCOUNT_SERVICE_URL', 'http://127.0.0.1:8085')

LEDGER_URL = os.environ.get(
    'LEDGER_URL', 'http://127.0.0.1:8008')

ENERGY_TYPE_SERVICE_URL = os.environ.get(
    'ENERGY_TYPE_SERVICE_URL', 'http://127.0.0.1:8765')


# -- Auth/tokens -------------------------------------------------------------

TOKEN_HEADER = 'Authorization'

HYDRA_URL = os.environ.get(
    'HYDRA_URL', 'https://localhost:9100')

HYDRA_INTROSPECT_URL = os.environ.get(
    'HYDRA_INTROSPECT_URL', 'https://localhost:9101/oauth2/introspect')


# -- Task broker and locking -------------------------------------------------

REDIS_HOST = os.environ.get('REDIS_HOST', '172.17.0.3')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_USERNAME = os.environ.get('REDIS_USERNAME', '')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', '')
REDIS_CACHE_DB = int(os.environ.get('REDIS_CACHE_DB', 6))
REDIS_BROKER_DB = int(os.environ.get('REDIS_BROKER_DB', 7))
REDIS_BACKEND_DB = int(os.environ.get('REDIS_BACKEND_DB', 8))

REDIS_URL = 'redis://%s:%s@%s:%d' % (
    REDIS_USERNAME, REDIS_PASSWORD, REDIS_HOST, REDIS_PORT)

REDIS_BROKER_URL = '%s/%d' % (REDIS_URL, REDIS_BROKER_DB)
REDIS_BACKEND_URL = '%s/%d' % (REDIS_URL, REDIS_BACKEND_DB)


# -- Misc --------------------------------------------------------------------

# TODO replace with ELOVERBLIK_REQUEST_ACCESS_FROM?
# FIRST_MEASUREMENT_TIME = datetime(2018, 1, 1, 0, 0, tzinfo=timezone.utc)
FIRST_MEASUREMENT_TIME = datetime(2019, 9, 15, 0, 0, tzinfo=timezone.utc)

GGO_EXPIRE_TIME = timedelta(days=90)
