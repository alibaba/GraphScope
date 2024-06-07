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
import com.alibaba.graphscope.interactive.openapi.model.*;

import org.apache.tinkerpop.gremlin.driver.Client;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class BasicExample {

    public static String createGraph(Session session) {
        CreateGraphRequest graph = new CreateGraphRequest();
        graph.setName("testGraph");
        graph.setDescription("a simple test graph");
        CreateGraphSchemaRequest schema = new CreateGraphSchemaRequest();
        {
            CreateVertexType vertexType = new CreateVertexType();
            vertexType.setTypeName("person");
            List<CreatePropertyMeta> propertyMetaList = new ArrayList<>();
            {
                CreatePropertyMeta propertyMeta = new CreatePropertyMeta();
                propertyMeta.setPropertyName("id");
                propertyMeta.setPropertyType(
                        new GSDataType(
                                new PrimitiveType()
                                        .primitiveType(
                                                PrimitiveType.PrimitiveTypeEnum.SIGNED_INT64)));
                propertyMetaList.add(propertyMeta);
            }
            {
                CreatePropertyMeta propertyMeta = new CreatePropertyMeta();
                propertyMeta.setPropertyName("name");
                propertyMeta.setPropertyType(
                        new GSDataType(
                                (new StringType()
                                        .string(
                                                new StringTypeString(
                                                        new LongText().longText(""))))));
                System.out.println("json: " + propertyMeta.toJson());
                propertyMetaList.add(propertyMeta);
            }
            {
                // age
                CreatePropertyMeta propertyMeta = new CreatePropertyMeta();
                propertyMeta.setPropertyName("age");
                propertyMeta.setPropertyType(
                        new GSDataType(
                                new PrimitiveType()
                                        .primitiveType(
                                                PrimitiveType.PrimitiveTypeEnum.SIGNED_INT32)));
                propertyMetaList.add(propertyMeta);
            }
            vertexType.setProperties(propertyMetaList);
            vertexType.addPrimaryKeysItem("id");
            schema.addVertexTypesItem(vertexType);
        }
        {
            CreateEdgeType edgeType = new CreateEdgeType();
            edgeType.setTypeName("knows");
            List<CreatePropertyMeta> propertyMetaList = new ArrayList<>();
            {
                CreatePropertyMeta propertyMeta = new CreatePropertyMeta();
                propertyMeta.setPropertyName("weight");
                propertyMeta.setPropertyType(
                        new GSDataType(
                                new PrimitiveType()
                                        .primitiveType(PrimitiveType.PrimitiveTypeEnum.DOUBLE)));
                propertyMetaList.add(propertyMeta);
            }
            edgeType.setProperties(propertyMetaList);
            BaseEdgeTypeVertexTypePairRelationsInner relationShip =
                    new BaseEdgeTypeVertexTypePairRelationsInner();
            relationShip.setSourceVertex("person");
            relationShip.setDestinationVertex("person");
            relationShip.relation(
                    BaseEdgeTypeVertexTypePairRelationsInner.RelationEnum.MANY_TO_MANY);
            edgeType.addVertexTypePairRelationsItem(relationShip);
            schema.addEdgeTypesItem(edgeType);
        }
        graph.setSchema(schema);
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

    public static String bulkLoading(Session session, String graphId) {
        SchemaMapping schemaMapping = new SchemaMapping();
        schemaMapping.setGraph(graphId);
        {
            SchemaMappingLoadingConfig loadingConfig = new SchemaMappingLoadingConfig();
            loadingConfig.setImportOption(SchemaMappingLoadingConfig.ImportOptionEnum.INIT);
            loadingConfig.setFormat(new SchemaMappingLoadingConfigFormat().type("csv"));
            schemaMapping.setLoadingConfig(loadingConfig);
        }
        {
            String personPath =
                    new File("../../../examples/modern_graph/person.csv").getAbsolutePath();
            String knowsPath =
                    new File("../../../examples/modern_graph/person_knows_person.csv")
                            .getAbsolutePath();
            {
                VertexMapping vertexMapping = new VertexMapping();
                vertexMapping.setTypeName("person");
                vertexMapping.addInputsItem(personPath);
                schemaMapping.addVertexMappingsItem(vertexMapping);
            }
            {
                EdgeMapping edgeMapping = new EdgeMapping();
                edgeMapping.setTypeTriplet(
                        new EdgeMappingTypeTriplet()
                                .edge("knows")
                                .sourceVertex("person")
                                .destinationVertex("person"));
                edgeMapping.addInputsItem(knowsPath);
                schemaMapping.addEdgeMappingsItem(edgeMapping);
            }
        }
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

        String graphId = createGraph(session);
        String jobId = bulkLoading(session, graphId);
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
        return;
    }
}
