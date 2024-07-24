#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
import time
import unittest

import pytest

from gs_interactive.client.driver import Driver
from gs_interactive.models import *


class TestDriver(unittest.TestCase):
    """Test usage of driver"""

    def setUp(self):
        # get endpoint from environment variable INTERACTIVE_ADMIN_ENDPOINT
        self._driver = Driver()
        self._sess = self._driver.getDefaultSession()
        self._gremlin_client = self._driver.getGremlinClient()
        self._graph_id = None
        self._cypher_proc_name = None
        self._cpp_proc_name = None
        print("finish setup")

    def tearDown(self):
        if self._graph_id is not None:
            if self._cypher_proc_name is not None:
                print("delete procedure: ")
                rep1 = self._sess.delete_procedure(
                    self._graph_id, self._cypher_proc_name
                )
                print("delete procedure: ", rep1)
            if self._cpp_proc_name is not None:
                print("delete procedure: ")
                rep1 = self._sess.delete_procedure(self._graph_id, self._cpp_proc_name)
                print("delete procedure: ", rep1)
            print("delete graph: ", self._graph_id)
            rep2 = self._sess.delete_graph(self._graph_id)
            print("delete graph: ", rep2)

    def test_example(self):
        self._graph_id = self.createGraph()
        self.bulkLoading()
        self.bulkLoadingUploading()
        self.bulkLoadingFailure()
        self.list_graph()
        self.get_graph_meta()
        self.runCypherQuery()
        self.runGremlinQuery()
        self.createCypherProcedure()
        self.createCppProcedure()
        self.restart()
        self.restartOnNewGraph()
        self.getStatistics()
        self.callProcedure()
        self.callProcedureWithHttp()
        self.callProcedureWithHttpCurrent()
        self.createDriver()

    def createGraph(self):
        create_graph = CreateGraphRequest(name="test_graph", description="test graph")
        create_schema = CreateGraphSchemaRequest()
        create_person_vertex = CreateVertexType(
            type_name="person",
            primary_keys=["id"],
            properties=[
                CreatePropertyMeta(
                    property_name="id",
                    property_type=GSDataType(
                        PrimitiveType(primitive_type="DT_SIGNED_INT64")
                    ),
                ),
                CreatePropertyMeta(
                    property_name="name",
                    property_type=GSDataType(
                        StringType(string=StringTypeString(LongText(long_text="")))
                    ),
                ),
                CreatePropertyMeta(
                    property_name="age",
                    property_type=GSDataType(
                        PrimitiveType(primitive_type="DT_SIGNED_INT32")
                    ),
                ),
            ],
        )
        create_schema.vertex_types = [create_person_vertex]
        create_knows_edge = CreateEdgeType(
            type_name="knows",
            properties=[
                CreatePropertyMeta(
                    property_name="weight",
                    property_type=GSDataType(PrimitiveType(primitive_type="DT_DOUBLE")),
                )
            ],
            vertex_type_pair_relations=[
                BaseEdgeTypeVertexTypePairRelationsInner(
                    source_vertex="person", destination_vertex="person"
                )
            ],
        )
        create_schema.edge_types = [create_knows_edge]
        create_graph.var_schema = create_schema
        resp = self._sess.create_graph(create_graph)
        assert resp.is_ok()
        return resp.get_value().graph_id

    def bulkLoading(self):
        assert os.environ.get("FLEX_DATA_DIR") is not None
        location = os.environ.get("FLEX_DATA_DIR")
        person_csv_path = "person.csv"
        knows_csv_path = "person_knows_person.csv"
        print("test bulk loading: ", self._graph_id)
        schema_mapping = SchemaMapping(
            loading_config=SchemaMappingLoadingConfig(
                data_source=SchemaMappingLoadingConfigDataSource(
                    scheme="file", location=location
                ),
                import_option="init",
                format=SchemaMappingLoadingConfigFormat(type="csv"),
                x_csr_params=SchemaMappingLoadingConfigXCsrParams(
                    parallelism=1, build_csr_in_mem=True, use_mmap_vector=True
                ),
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

    def bulkLoadingUploading(self):
        """
        Test bulk loading with uploading files
        """
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

    def waitJobFinish(self, job_id: str):
        assert job_id is not None
        while True:
            resp = self._sess.get_job(job_id)
            assert resp.is_ok()
            status = resp.get_value().status
            print("job status: ", status)
            if status == "SUCCESS":
                return True
            elif status == "FAILED":
                return False
            else:
                time.sleep(1)

    def bulkLoadingFailure(self):
        """
        Submit a bulk loading job with invalid data, and expect the job to fail.
        """
        assert os.environ.get("FLEX_DATA_DIR") is not None
        person_csv_path = os.path.join(os.environ.get("FLEX_DATA_DIR"), "person.csv")
        knows_csv_path = os.path.join(
            os.environ.get("FLEX_DATA_DIR"), "person_knows_person.csv"
        )
        print("test bulk loading: ", self._graph_id)
        schema_mapping = SchemaMapping(
            loading_config=SchemaMappingLoadingConfig(
                import_option="init",
                format=SchemaMappingLoadingConfigFormat(type="csv"),
            ),
            vertex_mappings=[
                # Intentionally use the wrong file for the vertex mapping
                VertexMapping(type_name="person", inputs=[knows_csv_path])
            ],
            edge_mappings=[
                EdgeMapping(
                    type_triplet=EdgeMappingTypeTriplet(
                        edge="knows",
                        source_vertex="person",
                        destination_vertex="person",
                    ),
                    # Intentionally use the wrong file for the edge mapping
                    inputs=[person_csv_path],
                )
            ],
        )
        resp = self._sess.bulk_loading(self._graph_id, schema_mapping)
        assert resp.is_ok()
        job_id = resp.get_value().job_id
        # Expect to fail
        assert self.waitJobFinish(job_id) == False

    def list_graph(self):
        resp = self._sess.list_graphs()
        assert resp.is_ok()
        print("list graph: ", resp.get_value())
    
    def get_graph_meta(self):
        resp = self._sess.get_graph_meta(self._graph_id)
        assert resp.is_ok()
        print("get graph meta: ", resp.get_value())
        # Now test calling with a int value, will be automatically converted to string
        resp = self._sess.get_graph_meta(1)
        assert resp.is_ok()
        # Now test calling with a invalid value, will raise exception
        with self.assertRaises(Exception) as context:
            resp = self._sess.get_graph_meta([1,2,3])


    def runCypherQuery(self):
        query = "MATCH (n) RETURN COUNT(n);"
        with self._driver.getNeo4jSession() as session:
            resp = session.run(query)
            print("cypher query result: ", resp)

    def runGremlinQuery(self):
        query = "g.V().count();"
        ret = []
        q = self._gremlin_client.submit(query)
        while True:
            try:
                ret.extend(q.next())
            except StopIteration:
                break
        print(ret)

    def createCypherProcedure(self):
        self._cypher_proc_name = "test_procedure"
        create_proc_request = CreateProcedureRequest(
            name=self._cypher_proc_name,
            description="test procedure",
            query="MATCH (n) RETURN COUNT(n);",
            type="cypher",
        )
        resp = self._sess.create_procedure(self._graph_id, create_proc_request)
        assert resp.is_ok()
        print("create procedure: ", resp.get_value())

    def createCppProcedure(self):
        self._cpp_proc_name = "test_procedure_cpp"
        # read strings from file ../../java/src/test/resources/sample_app.cc
        app_path = os.path.join(
            os.path.dirname(__file__), "../../java/src/test/resources/sample_app.cc"
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

    def restart(self):
        resp = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=self._graph_id)
        )
        assert resp.is_ok()
        print("restart: ", resp.get_value())
        # wait 5 seconds
        time.sleep(5)
        # get service status
        resp = self._sess.get_service_status()
        assert resp.is_ok()
        print("get service status: ", resp.get_value())

    def restartOnNewGraph(self):
        original_graph_id = None
        status_res = self._sess.get_service_status()
        assert status_res.is_ok()
        status = status_res.get_value()
        if status.status == "Running":
            if status.graph is not None and status.graph.id is not None:
                original_graph_id = status.graph.id
            else:
                raise Exception("service status error, graph id is None")
        elif status.status == "Stopped":
            pass
        else:
            raise Exception("service status error " + status)
        assert original_graph_id is not None
        # create new graph
        new_graph_id = self.createGraph()
        # start service
        print("start service on new graph: ", new_graph_id)
        start_service_res = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=new_graph_id)
        )
        assert start_service_res.is_ok()
        # restart service
        print("restart service on new graph: ", new_graph_id)
        restart_res = self._sess.restart_service()
        assert restart_res.is_ok()
        # get status
        print("get service status: ")
        status_res = self._sess.get_service_status()
        assert status_res.is_ok()
        print("get service status: ", status_res.get_value().status)
        # If we don't stop service, delete graph will fail
        delete_res = self._sess.delete_graph(new_graph_id)
        assert not delete_res.is_ok()
        delete_failure_msg = delete_res.get_status_message()
        # expect "Graph is runnning" in the error message
        print("delete graph failed: ", delete_failure_msg)
        assert "Graph is running" in delete_failure_msg

        # stop
        print("stop service: ")
        stop_res = self._sess.stop_service()
        assert stop_res.is_ok()
        # get status
        print("get service status: ")
        status_res = self._sess.get_service_status()
        assert status_res.is_ok()
        print("get service status: ", status_res.get_value().status)
        assert status_res.get_value().status == "Stopped"
        # after stop, we should be able to delete the graph
        print("delete graph: ", new_graph_id)
        delete_res = self._sess.delete_graph(new_graph_id)
        assert delete_res.is_ok()
        # start on original graph
        print("start service on original graph: ", original_graph_id)
        start_service_res = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=original_graph_id)
        )
        assert start_service_res.is_ok()
        print("finish restartOnNewGraph")
        time.sleep(5)

    def getStatistics(self):
        resp = self._sess.get_graph_statistics(self._graph_id)
        assert resp.is_ok()
        print("get graph statistics: ", resp.get_value())

    def callProcedure(self):
        with self._driver.getNeo4jSession() as session:
            result = session.run("CALL test_procedure();")
            print("call procedure result: ", result)
    
    def callPrcedureWithServiceStop(self):
        # stop service
        print("stop service: ")
        stop_res = self._sess.stop_service()
        assert stop_res.is_ok()
        # call procedure on stopped service should raise exception
        with self._driver.getNeo4jSession() as session:
            with self.assertRaises(Exception) as context:
                result = session.run("CALL test_procedure();")
        # start service
        print("start service: ")
        start_res = self._sess.start_service(
            start_service_request=StartServiceRequest(graph_id=self._graph_id)
        )
        assert start_res.is_ok()
        # wait 5 seconds
        time.sleep(5)

    def callProcedureWithHttp(self):
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

    def callProcedureWithHttpCurrent(self):
        req = QueryRequest(
            query_name=self._cpp_proc_name,
            arguments=[
                TypedValue(
                    type=GSDataType(PrimitiveType(primitive_type="DT_SIGNED_INT32")),
                    value=1,
                )
            ],
        )
        resp = self._sess.call_procedure_current(params=req)
        assert resp.is_ok()
        print("call procedure result: ", resp.get_value())

    def createDriver(self):
        driver = Driver()
        sess = driver.getDefaultSession()
        print("create driver: ", sess)


if __name__ == "__main__":
    unittest.main()
