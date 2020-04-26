import sys
import logging

from flask import Flask
from flask_cors import CORS

from .urls import urls
from .settings import DEBUG, CORS_ORIGINS

# Import models here for SQLAlchemy to detech them
from .models import VERSIONED_DB_MODELS


# -- Logging -----------------------------------------------------------------

if DEBUG:
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


# -- Flask setup -------------------------------------------------------------

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'

cors = CORS(app, resources={r'*': {'origins': CORS_ORIGINS}})


# -- URLs/routes setup -------------------------------------------------------

for url, controller in urls:
    app.add_url_rule(url, url, controller, methods=[controller.METHOD])
