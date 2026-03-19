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
from gs_interactive_admin.test.conftest import call_procedure
from neo4j import GraphDatabase

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

    def setup_class(self):
        if os.environ.get("INTERACTIVE_ADMIN_ENDPOINT") is None:
            raise Exception("INTERACTIVE_ADMIN_ENDPOINT is not set")
        self._endpoint = os.environ.get("INTERACTIVE_ADMIN_ENDPOINT")
        logger.info("Endpoint: %s", self._endpoint)
        self._driver = Driver(self._endpoint)
        self._sess = self._driver.getDefaultSession()
        self._graph_id = "33"

    def test1(self):
        # self.create_graph()
        # self.import_data()
        # self.get_statistics()
        # self.start_service()
        # self.call_builtin_procedure()
        self.create_cpp_procedure()
        # self.start_service_on_graph(self._graph_id)
        # self.call_cpp_procedure()
        # self.stop_service()
        # self.callVertexEdgeQuery()
        
    def start_service_on_graph(self, graph_id):
        resp = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=graph_id)
        )
        assert resp.is_ok()
        logger.info(f"Launching service resp {resp}")
        # get service status
        resp = self._sess.get_service_status()
        assert resp.is_ok()

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
        
    def get_statistics(self):
        resp = self._sess.get_graph_statistics(self._graph_id)
        assert resp.is_ok()
        print("Statistics: ", resp.get_value())

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
        # logger.info("Stopping service")
        # # logger.info(f"Wil wait 10 seconds before stopping service")
        # # time.sleep(200)
        resp = self._sess.stop_service(graph_id=self._graph_id)
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

    def call_builtin_procedure(self):
        """Test calling builtin procedure. The request should distributed to all valid backend servers in a roundrobin or a random manner."""
        all_endpoints = self._driver.getNeo4jEndpoints(self._graph_id)
        logger.info("All endpoints: %s", all_endpoints)
        for endpoint in all_endpoints:
            logger.info("Endpoint: %s", endpoint)
            neo4j_driver = GraphDatabase.driver(endpoint, auth=None)
            with neo4j_driver.session() as session:
                logger.info(f"calling procedure count vertices on graph {self._graph_id}")
                call_procedure(
                    session,
                    self._graph_id,
                    "count_vertices",
                    '"person"',
                )
                
    def create_cpp_procedure(self):
        self._cpp_proc_name = "test_procedure_cpp"
        app_path = os.path.join(
            os.path.dirname(__file__), "../../../java/src/test/resources/sample_app.cc"
        )
        if not os.path.exists(app_path):
            raise Exception("sample_app.cc not found")
        with open(app_path, "r") as f:
            app_content = f.read()

        create_proc_request = CreateProcedureRequest(
            name=self._cpp_proc_name,
            description="test procedure",
            query=app_content,
            type="cpp",
        )
        resp = self._sess.create_procedure(self._graph_id, create_proc_request)
        assert resp.is_ok()
        print("create procedure: ", resp.get_value())
        
    def call_cpp_procedure(self):
        """
        Test directly submit procedure request to query port.
        This will fetch the procedure's endpoints in a certain manner(roundrobin or random) and submit the request to the endpoint.
        """
        req = QueryRequest(
            query_name=self._cpp_proc_name,
            arguments=[
                TypedValue(
                    type=GSDataType(PrimitiveType(primitive_type="DT_SIGNED_INT32")),
                    value=1,
                )
            ],
        )
        resp = self._sess.call_procedure(graph_id=self._graph_id, params=req)
        assert resp.is_ok()
        print("call procedure result: ", resp.get_value())
        
    def callVertexEdgeQuery(self):
        """
        Vertex/Edge Queries could be splited into two kinds:
        - Write: insert/update/delete
        - Read: get
        For Write queries, we need to send to the primary server.
        For Read queries, we could send to any server.
        """
        # get vertex
        resp = self._sess.get_vertex(
            graph_id=self._graph_id,
            label="person",
            primary_key_value="1" # marko
        )
        assert resp.is_ok()
        vertex = resp.get_value()
        assert vertex.to_dict == {
            "label": "person",
            "values": [
                {"name": "id", "value": 1},
                {"name": "name", "value": "marko"},
                {"name": "age", "value": 29},
            ],
        }
        
    def tearDown(self):
        if self._graph_id is not None:
            rep1 = self.stop_service()
            print("stop service: ", rep1)

    def teardown_class(self):
        pass
        # if self._graph_id is not None:
            # rep2 = self._sess.delete_graph(self._graph_id)
            # print("delete graph: ", rep2)
        


if __name__ == "__main__":
    unittest.main()
