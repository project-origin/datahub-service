import logging
from flask import Flask
from flask_cors import CORS
from opencensus.trace import config_integration
from opencensus.ext.flask.flask_middleware import FlaskMiddleware

from .urls import urls
from .logger import handler, exporter, sampler
from .settings import SECRET, CORS_ORIGINS, SERVICE_NAME
from .tasks import celery_app

# Import models here for SQLAlchemy to detech them
from .models import VERSIONED_DB_MODELS


config_integration.trace_integrations(['requests'])
config_integration.trace_integrations(['sqlalchemy'])


celery_app.set_default()


# -- Flask setup -------------------------------------------------------------

app = Flask(SERVICE_NAME)
app.config['SECRET_KEY'] = SECRET
app.config['PROPAGATE_EXCEPTIONS'] = False
cors = CORS(app, resources={r'*': {'origins': CORS_ORIGINS}})

if handler:
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)

if exporter and sampler:
    opencensus = FlaskMiddleware(app, sampler=sampler, exporter=exporter)


# -- URLs/routes setup -------------------------------------------------------

for url, controller in urls:
    app.add_url_rule(url, url, controller, methods=[controller.METHOD])
