import unittest

from flask import json
import pytest
import logging
import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../python/"))

logger = logging.getLogger("interactive")

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase

from gs_interactive.client.driver import Driver  # noqa: E402
from gs_interactive.client.status import StatusCode  # noqa: E402
# Must use the models on SDK side
from gs_interactive.models import *

test_graph_def = {
    "name": "modern_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"var_char": {"max_length": 16}}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
            }
        ],
    },
}

class TestEndToEnd(unittest.TestCase):
    """A comprehensive test case contains creating graph, importing data, running queries, and deleting graph"""
    
    def setUp(self):
        if os.environ.get("INTERACTIVE_ENDPOINT") is None:
            raise Exception("INTERACTIVE_ENDPOINT is not set")
        self._endpoint = os.environ.get("INTERACTIVE_ENDPOINT")
        logger.info("Endpoint: %s", self._endpoint)
        self._driver = Driver(self._endpoint)
        self._sess = self._driver.getDefaultSession()
        self._graph_id = None
        
    def test1(self):
        self.create_graph()
        self.import_data()
        self.start_service()
        self.stop_service()
    

    def create_graph(self):
        create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
        resp = self._sess.create_graph(create_graph_request)
        assert resp.is_ok()
        graph_id = resp.get_value().graph_id
        print("Graph id: ", graph_id)
        self._graph_id = graph_id
    

    def import_data(self):
        assert os.environ.get("FLEX_DATA_DIR") is not None
        location = os.environ.get("FLEX_DATA_DIR")
        person_csv_path = "@/{}/person.csv".format(location)
        knows_csv_path = "@/{}/person_knows_person.csv".format(location)
        print("test bulk loading: ", self._graph_id, person_csv_path, knows_csv_path)
        schema_mapping = SchemaMapping(
            loading_config=SchemaMappingLoadingConfig(
                data_source=SchemaMappingLoadingConfigDataSource(scheme="file"),
                import_option="init",
                format=SchemaMappingLoadingConfigFormat(type="csv"),
            ),
            vertex_mappings=[
                VertexMapping(type_name="person", inputs=[person_csv_path])
            ],
            edge_mappings=[
                EdgeMapping(
                    type_triplet=EdgeMappingTypeTriplet(
                        edge="knows",
                        source_vertex="person",
                        destination_vertex="person",
                    ),
                    inputs=[knows_csv_path],
                )
            ],
        )
        resp = self._sess.bulk_loading(self._graph_id, schema_mapping)
        assert resp.is_ok()
        job_id = resp.get_value().job_id
        assert self.waitJobFinish(job_id)
        logger.info("Successfully import data")
    
    def start_service(self):
        resp = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=self._graph_id)
        )
        assert resp.is_ok()
        logger.info(f"Launching service resp {resp}")
        # get service status
        resp = self._sess.get_service_status()
        assert resp.is_ok()
    
    
    
    def stop_service(self):
        logger.info("Stopping service")
        resp = self._sess.stop_service(graph_id=self._graph_id)
        assert resp.is_ok()
        logger.info(f"Stop service resp {resp}")
        
    
    def waitJobFinish(self, job_id: str):
        assert job_id is not None
        while True:
            resp = self._sess.get_job(job_id)
            assert resp.is_ok()
            status = resp.get_value().status
            print("job status: ", status)
            if status == "SUCCESS":
                print(resp.get_value())
                return True
            elif status == "FAILED":
                return False
            else:
                time.sleep(1)
    
    
    def tearDown(self):
        if self._graph_id is not None:
            rep2 = self._sess.delete_graph(self._graph_id)
            print("delete graph: ", rep2)



if __name__ == "__main__":
    unittest.main()
