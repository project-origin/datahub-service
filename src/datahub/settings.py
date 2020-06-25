import os


__current_file = os.path.abspath(__file__)
__current_folder = os.path.split(__current_file)[0]


PROJECT_DIR = os.path.abspath(os.path.join(__current_folder, '..', '..'))
VAR_DIR = os.path.join(PROJECT_DIR, 'var')
SOURCE_DIR = os.path.join(PROJECT_DIR, 'src')
MIGRATIONS_DIR = os.path.join(SOURCE_DIR, 'migrations')
ALEMBIC_CONFIG_PATH = os.path.join(MIGRATIONS_DIR, 'alembic.ini')


if os.environ.get('TEST') in ('1', 't', 'true', 'yes'):
    from .settings_test import *
else:
    from .settings_default import *
