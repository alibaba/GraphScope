import logging

import connexion
from flask_testing import TestCase

from gs_interactive_admin.encoder import JSONEncoder
from gs_interactive_admin.core.service_discovery.service_registry import (
    initialize_service_registry,
)
from gs_interactive_admin.core.config import Config


class BaseTestCase(TestCase):

    def create_app(self):
        logging.basicConfig(level=logging.INFO)
        config = Config()
        config.service_registry.ttl = 3

        initialize_service_registry(config)
        logging.getLogger("connexion.operation").setLevel("ERROR")
        logging.getLogger("interactive").setLevel("INFO")
        app = connexion.App(__name__, specification_dir="../openapi/")
        app.app.json_encoder = JSONEncoder
        app.add_api("openapi.yaml", pythonic_params=True)

        return app.app
