import logging
from flask import Flask
from flask_cors import CORS
from opencensus.trace import config_integration
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.trace.samplers import ProbabilitySampler

from .urls import urls
from .logger import handler
from .settings import (
    SECRET,
    CORS_ORIGINS,
    AZURE_APP_INSIGHTS_CONN_STRING,
    PROJECT_NAME,
)

# Import models here for SQLAlchemy to detech them
from .models import VERSIONED_DB_MODELS


config_integration.trace_integrations(['requests'])
config_integration.trace_integrations(['sqlalchemy'])


# -- Flask setup -------------------------------------------------------------

app = Flask(PROJECT_NAME)
app.config['SECRET_KEY'] = SECRET
app.config['PROPAGATE_EXCEPTIONS'] = False

if handler is not None:
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)

if AZURE_APP_INSIGHTS_CONN_STRING:
    middleware = FlaskMiddleware(
        app,
        exporter=AzureExporter(connection_string=AZURE_APP_INSIGHTS_CONN_STRING),
        sampler=ProbabilitySampler(rate=1.0),
    )

cors = CORS(app, resources={r'*': {'origins': CORS_ORIGINS}})


# -- URLs/routes setup -------------------------------------------------------

for url, controller in urls:
    app.add_url_rule(url, url, controller, methods=[controller.METHOD])
