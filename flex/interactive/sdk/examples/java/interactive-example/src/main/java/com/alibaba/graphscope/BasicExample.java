/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope;

import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.models.*;

import org.apache.tinkerpop.gremlin.driver.Client;

import java.io.IOException;
import java.util.List;

/**
 * Hello world!
 */
public class BasicExample {

    private static final String MODERN_GRAPH_SCHEMA_JSON =
            "{\n"
                    + "    \"name\": \"modern_graph\",\n"
                    + "    \"description\": \"This is a test graph\",\n"
                    + "    \"schema\": {\n"
                    + "        \"vertex_types\": [\n"
                    + "            {\n"
                    + "                \"type_name\": \"person\",\n"
                    + "                \"properties\": [\n"
                    + "                    {\n"
                    + "                        \"property_name\": \"id\",\n"
                    + "                        \"property_type\": {\"primitive_type\":"
                    + " \"DT_SIGNED_INT64\"},\n"
                    + "                    },\n"
                    + "                    {\n"
                    + "                        \"property_name\": \"name\",\n"
                    + "                        \"property_type\": {\"string\": {\"long_text\":"
                    + " \"\"}},\n"
                    + "                    },\n"
                    + "                    {\n"
                    + "                        \"property_name\": \"age\",\n"
                    + "                        \"property_type\": {\"primitive_type\":"
                    + " \"DT_SIGNED_INT32\"},\n"
                    + "                    },\n"
                    + "                ],\n"
                    + "                \"primary_keys\": [\"id\"],\n"
                    + "            }\n"
                    + "        ],\n"
                    + "        \"edge_types\": [\n"
                    + "            {\n"
                    + "                \"type_name\": \"knows\",\n"
                    + "                \"vertex_type_pair_relations\": [\n"
                    + "                    {\n"
                    + "                        \"source_vertex\": \"person\",\n"
                    + "                        \"destination_vertex\": \"person\",\n"
                    + "                        \"relation\": \"MANY_TO_MANY\",\n"
                    + "                    }\n"
                    + "                ],\n"
                    + "                \"properties\": [\n"
                    + "                    {\n"
                    + "                        \"property_name\": \"weight\",\n"
                    + "                        \"property_type\": {\"primitive_type\":"
                    + " \"DT_DOUBLE\"},\n"
                    + "                    }\n"
                    + "                ],\n"
                    + "                \"primary_keys\": [],\n"
                    + "            }\n"
                    + "        ],\n"
                    + "    },\n"
                    + "}";

    // Remember to replace the path with your own file path
    private static final String MODERN_GRAPH_BULK_LOADING_JSON =
            "{\n"
                + "    \"vertex_mappings\": [\n"
                + "        {\n"
                + "            \"type_name\": \"person\",\n"
                + "            \"inputs\": [\"@/tmp/person.csv\"],\n"
                + "            \"column_mappings\": [\n"
                + "                {\"column\": {\"index\": 0, \"name\": \"id\"}, \"property\":"
                + " \"id\"},\n"
                + "                {\"column\": {\"index\": 1, \"name\": \"name\"}, \"property\":"
                + " \"name\"},\n"
                + "                {\"column\": {\"index\": 2, \"name\": \"age\"}, \"property\":"
                + " \"age\"},\n"
                + "            ],\n"
                + "        }\n"
                + "    ],\n"
                + "    \"edge_mappings\": [\n"
                + "        {\n"
                + "            \"type_triplet\": {\n"
                + "                \"edge\": \"knows\",\n"
                + "                \"source_vertex\": \"person\",\n"
                + "                \"destination_vertex\": \"person\",\n"
                + "            },\n"
                + "            \"inputs\": [\n"
                + "                \"@/tmp/person_knows_person.csv\"\n"
                + "            ],\n"
                + "            \"source_vertex_mappings\": [\n"
                + "                {\"column\": {\"index\": 0, \"name\": \"person.id\"},"
                + " \"property\": \"id\"}\n"
                + "            ],\n"
                + "            \"destination_vertex_mappings\": [\n"
                + "                {\"column\": {\"index\": 1, \"name\": \"person.id\"},"
                + " \"property\": \"id\"}\n"
                + "            ],\n"
                + "            \"column_mappings\": [\n"
                + "                {\"column\": {\"index\": 2, \"name\": \"weight\"}, \"property\":"
                + " \"weight\"}\n"
                + "            ],\n"
                + "        }\n"
                + "    ],\n"
                + "}";

    public static String createGraph(Session session) throws IOException {
        CreateGraphRequest graph = CreateGraphRequest.fromJson(MODERN_GRAPH_SCHEMA_JSON);
        Result<CreateGraphResponse> rep = session.createGraph(graph);
        if (rep.isOk()) {
            System.out.println("create graph success");
        } else {
            throw new RuntimeException("create graph failed: " + rep.getStatusMessage());
        }
        String graphId = rep.getValue().getGraphId();
        System.out.println("graphId: " + graphId);
        return graphId;
    }

    public static String bulkLoading(Session session, String graphId) throws IOException {
        SchemaMapping schemaMapping = SchemaMapping.fromJson(MODERN_GRAPH_BULK_LOADING_JSON);
        Result<JobResponse> rep = session.bulkLoading(graphId, schemaMapping);
        if (rep.isOk()) {
            System.out.println("bulk loading success");
        } else {
            throw new RuntimeException("bulk loading failed: " + rep.getStatusMessage());
        }
        String jobId = rep.getValue().getJobId();
        System.out.println("job id: " + jobId);
        return jobId;
    }

    public static void waitJobFinished(Session session, String jobId) {
        if (jobId == null) {
            return;
        }
        while (true) {
            Result<JobStatus> rep = session.getJobStatus(jobId);
            if (!rep.isOk()) {
                throw new RuntimeException("get job status failed: " + rep.getStatusMessage());
            }
            JobStatus job = rep.getValue();
            if (job.getStatus() == JobStatus.StatusEnum.SUCCESS) {
                System.out.println("job finished");
                break;
            } else if (job.getStatus() == JobStatus.StatusEnum.FAILED) {
                throw new RuntimeException("job failed");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        // get endpoint from command line
        if (args.length != 1) {
            System.out.println("Usage: java -jar <jar> <endpoint>");
            return;
        }
        String endpoint = args[0];
        Driver driver = Driver.connect(endpoint);
        Session session = driver.session();

        String graphId = null;
        try {
            graphId = createGraph(session);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String jobId = null;
        try {
            jobId = bulkLoading(session, graphId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        waitJobFinished(session, jobId);
        System.out.println("bulk loading finished");

        // start the service on the created graph
        Result<String> startServiceResponse =
                session.startService(new StartServiceRequest().graphId(graphId));
        if (startServiceResponse.isOk()) {
            System.out.println("start service success");
        } else {
            throw new RuntimeException(
                    "start service failed: " + startServiceResponse.getStatusMessage());
        }

        // run cypher query
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("MATCH(a) return COUNT(a);");
            System.out.println("result: " + result.toString());
        }

        // run gremlin query
        Client gremlinClient = driver.getGremlinClient();
        try {
            List<org.apache.tinkerpop.gremlin.driver.Result> results =
                    gremlinClient.submit("g.V().count()").all().get();
            System.out.println("result: " + results.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Advanced features, creating a procedure
        CreateProcedureRequest procedure =
                new CreateProcedureRequest()
                        .name("testProcedure")
                        .description("a simple test procedure")
                        .query("MATCH(p:person) RETURN COUNT(p);")
                        .type(CreateProcedureRequest.TypeEnum.CYPHER);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        if (resp.isOk()) {
            System.out.println("create procedure success");
        } else {
            throw new RuntimeException("create procedure failed: " + resp.getStatusMessage());
        }

        // restart the service to make the procedure take effect
        Result<String> restartResp = session.restartService();
        if (restartResp.isOk()) {
            System.out.println("service restarted: " + restartResp.getValue());
        } else {
            throw new RuntimeException("restart service failed: " + restartResp.getStatusMessage());
        }
        // Sleep 5 seconds to wait for the service to restart
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // run the procedure
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("CALL testProcedure() YIELD *;");
            System.out.println("result: " + result.toString());
        }
        System.out.println("Finish all tests");

        // Delete the graph
        Result<String> deleteGraphResponse = session.deleteGraph(graphId);
        if (deleteGraphResponse.isOk()) {
            System.out.println("delete graph success");
        } else {
            throw new RuntimeException(
                    "delete graph failed: " + deleteGraphResponse.getStatusMessage());
        }
    }
}
