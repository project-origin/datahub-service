import os
import sys
from logging.config import fileConfig
from sqlalchemy import MetaData
from alembic import context

sys.path.append(os.path.join(os.path.abspath(os.path.split(os.path.abspath(__file__))[0]), '..'))

from datahub.db import engine
from datahub.models import VERSIONED_DB_MODELS


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)


def run_migrations():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    def __combine_metadata(models):
        m = MetaData()
        for model in models:
            for t in model.metadata.tables.values():
                t.tometadata(m)
        return m

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=__combine_metadata(VERSIONED_DB_MODELS),
        )

        with context.begin_transaction():
            context.run_migrations()


run_migrations()
