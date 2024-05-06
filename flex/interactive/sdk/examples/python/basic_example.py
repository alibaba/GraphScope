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
import time
import argparse
import os
from interactive_sdk.openapi.models.long_text import LongText
from interactive_sdk.openapi.models.edge_mapping_type_triplet import (
    EdgeMappingTypeTriplet,
)
from interactive_sdk.client.driver import Driver
from interactive_sdk.client.session import Session
from interactive_sdk.openapi.models.base_edge_type_vertex_type_pair_relations_inner import (
    BaseEdgeTypeVertexTypePairRelationsInner,
)
from interactive_sdk.openapi.models.create_edge_type import CreateEdgeType
from interactive_sdk.openapi.models.create_graph_request import CreateGraphRequest
from interactive_sdk.openapi.models.create_graph_schema_request import (
    CreateGraphSchemaRequest,
)
from interactive_sdk.openapi.models.create_procedure_request import (
    CreateProcedureRequest,
)
from interactive_sdk.openapi.models.create_property_meta import CreatePropertyMeta
from interactive_sdk.openapi.models.create_vertex_type import CreateVertexType
from interactive_sdk.openapi.models.edge_mapping import EdgeMapping
from interactive_sdk.openapi.models.gs_data_type import GSDataType
from interactive_sdk.openapi.models.start_service_request import StartServiceRequest
from interactive_sdk.openapi.models.primitive_type import PrimitiveType
from interactive_sdk.openapi.models.schema_mapping import SchemaMapping
from interactive_sdk.openapi.models.schema_mapping_loading_config import (
    SchemaMappingLoadingConfig,
)
from interactive_sdk.openapi.models.schema_mapping_loading_config_format import (
    SchemaMappingLoadingConfigFormat,
)
from interactive_sdk.openapi.models.string_type import StringType
from interactive_sdk.openapi.models.string_type_string import StringTypeString
from interactive_sdk.openapi.models.vertex_mapping import VertexMapping


def createGraph(sess: Session):
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
    resp = sess.create_graph(create_graph)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    print("create graph: ", graph_id)
    return graph_id


def bulkLoading(sess: Session, graph_id: str):
    person_csv_path = os.path.abspath("../../../examples/modern_graph/person.csv")
    knows_csv_path = os.path.abspath(
        "../../../examples/modern_graph/person_knows_person.csv"
    )
    schema_mapping = SchemaMapping(
        graph=graph_id,
        loading_config=SchemaMappingLoadingConfig(
            import_option="init",
            format=SchemaMappingLoadingConfigFormat(type="csv"),
        ),
        vertex_mappings=[VertexMapping(type_name="person", inputs=[person_csv_path])],
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
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    return job_id


def waitJobFinish(sess: Session, job_id: str):
    while True:
        resp = sess.get_job(job_id)
        assert resp.is_ok()
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            break
        elif status == "FAILED":
            raise Exception("job failed")
        else:
            time.sleep(1)


if __name__ == "__main__":
    # expect one argument: interactive_endpoint
    parser = argparse.ArgumentParser(description="Example Python3 script")

    # Add arguments
    parser.add_argument(
        "--endpoint",
        type=str,
        help="The interactive endpoint to connect",
        required=True,
        default="https://virtserver.swaggerhub.com/GRAPHSCOPE/interactive/1.0.0/",
    )

    # Parse the arguments
    args = parser.parse_args()

    driver = Driver(endpoint=args.endpoint)
    with driver.session() as sess:
        graph_id = createGraph(sess)
        job_id = bulkLoading(sess, graph_id)
        waitJobFinish(sess, job_id)
        print("bulk loading finished")

        # Now start service on the created graph.
        resp = sess.start_service(
            start_service_request=StartServiceRequest(graph_id=graph_id)
        )
        assert resp.is_ok()
        time.sleep(5)
        print("restart service on graph ", graph_id)

        # running a simple cypher query
        query = "MATCH (n) RETURN COUNT(n);"
        with driver.getNeo4jSession() as session:
            resp = session.run(query)
            for record in resp:
                print(record)

        # running a simple gremlin query
        query = "g.V().count();"
        ret = []
        gremlin_client = driver.getGremlinClient()
        q = gremlin_client.submit(query)
        while True:
            try:
                ret.extend(q.next())
            except StopIteration:
                break
        print(ret)

        # more advanced usage of procedure
        create_proc_request = CreateProcedureRequest(
            name="test_procedure",
            description="test procedure",
            query="MATCH (n) RETURN COUNT(n);",
            type="cypher",
        )
        resp = sess.create_procedure(graph_id, create_proc_request)
        assert resp.is_ok()

        # must start service on the current graph, to let the procedure take effect
        resp = sess.restart_service()
        assert resp.is_ok()
        print("restarted service on graph ", graph_id)
        time.sleep(5)

        # Now call the procedure
        with driver.getNeo4jSession() as session:
            result = session.run("CALL test_procedure();")
            for record in result:
                print(record)
