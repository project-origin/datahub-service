import logging

from flask import Flask
from flask_cors import CORS

from .urls import urls
from .logger import handler
from .settings import SECRET, CORS_ORIGINS

# Import models here for SQLAlchemy to detech them
from .models import VERSIONED_DB_MODELS


# -- Flask setup -------------------------------------------------------------

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET
app.config['PROPAGATE_EXCEPTIONS'] = False

if handler is not None:
    handler.setLevel(logging.DEBUG)
    app.logger.addHandler(handler)

cors = CORS(app, resources={r'*': {'origins': CORS_ORIGINS}})


# -- URLs/routes setup -------------------------------------------------------

for url, controller in urls:
    app.add_url_rule(url, url, controller, methods=[controller.METHOD])
