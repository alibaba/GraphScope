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
package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.utils.Encoder;
import com.alibaba.graphscope.interactive.models.*;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DriverTest {

    private static final Logger logger = Logger.getLogger(DriverTest.class.getName());

    private static Driver driver;
    private static Session session;
    private static org.neo4j.driver.Session neo4jSession;
    private static Client gremlinClient;
    private static String graphId;
    private static String jobId;
    private static String cypherProcedureId;
    private static String cppProcedureId1;
    private static String cppProcedureId2;
    private static String personNameValue = "marko";

    @BeforeAll
    public static void beforeClass() {
        String interactiveEndpoint =
                System.getProperty("interactive.endpoint", "http://localhost:7777");
        driver = Driver.connect(interactiveEndpoint);
        assert driver != null;
        System.out.println("driver is not null: " + (driver != null));
        session = driver.session();
        String neo4jEndpoint = driver.getNeo4jEndpoint();
        if (neo4jEndpoint != null) {
            neo4jSession = driver.getNeo4jSession();
        }
        Pair<String, Integer> gremlinEndpoint = driver.getGremlinEndpoint();
        if (gremlinEndpoint != null) {
            gremlinClient = driver.getGremlinClient();
        }
        logger.info("Finish setup");
    }

    @AfterAll
    public static void afterClass() {
        logger.info("clean up");
        {
            Result<String> resp = session.startService(new StartServiceRequest().graphId("1"));
            assertOk(resp);
            logger.info("service restarted on initial graph");
        }
        if (graphId != null) {
            if (cypherProcedureId != null) {
                Result<String> resp = session.deleteProcedure(graphId, cypherProcedureId);
                logger.info("cypherProcedure deleted: " + resp.getValue());
            }
            if (cppProcedureId1 != null) {
                Result<String> resp = session.deleteProcedure(graphId, cppProcedureId1);
                logger.info("cppProcedure1 deleted: " + resp.getValue());
            }
            if (cppProcedureId2 != null) {
                Result<String> resp = session.deleteProcedure(graphId, cppProcedureId2);
                logger.info("cppProcedure2 deleted: " + resp.getValue());
            }
            Result<String> resp = session.deleteGraph(graphId);
            assertOk(resp);
            logger.info("graph deleted: " + resp.getValue());
        }
    }

    @Test
    public void test() {
        test0CreateGraph();
        test1BulkLoading();
        test2BulkLoadingUploading();
        test3StartService();
        test4CypherAdhocQuery();
        test5GremlinAdhoQuery();
        test6CreateCypherProcedure();
        test7CreateCppProcedure1();
        test7CreateCppProcedure2();
        test8Restart();
        test9GetGraphStatistics();
        test9CallCppProcedureJson();
        test9CallCppProcedure1Current();
        test9CallCppProcedure2();
        test10CallCypherProcedureViaNeo4j();
        testQueryInterface();
        test11CreateDriver();
    }

    public void test0CreateGraph() {
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
                logger.info("json: " + propertyMeta.toJson());
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
        assertOk(rep);
        graphId = rep.getValue().getGraphId();
        logger.info("graphId: " + graphId);
    }

    public void test1BulkLoading() {
        SchemaMapping schemaMapping = new SchemaMapping();
        {
            SchemaMappingLoadingConfig loadingConfig = new SchemaMappingLoadingConfig();
            loadingConfig.setImportOption(SchemaMappingLoadingConfig.ImportOptionEnum.INIT);
            loadingConfig.setFormat(new SchemaMappingLoadingConfigFormat().type("csv"));
            schemaMapping.setLoadingConfig(loadingConfig);
        }
        {
            // get env var FLEX_DATA_DIR
            if (System.getenv("FLEX_DATA_DIR") == null) {
                logger.info("FLEX_DATA_DIR is not set");
                return;
            }
            String personPath = System.getenv("FLEX_DATA_DIR") + "/person.csv";
            String knowsPath = System.getenv("FLEX_DATA_DIR") + "/person_knows_person.csv";
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
        assertOk(rep);
        jobId = rep.getValue().getJobId();
        logger.info("job id: " + jobId);
        waitJobFinished(jobId);
    }

    public void test2BulkLoadingUploading() {
        SchemaMapping schemaMapping = new SchemaMapping();
        {
            SchemaMappingLoadingConfig loadingConfig = new SchemaMappingLoadingConfig();
            loadingConfig.setImportOption(SchemaMappingLoadingConfig.ImportOptionEnum.INIT);
            loadingConfig.setFormat(new SchemaMappingLoadingConfigFormat().type("csv"));
            schemaMapping.setLoadingConfig(loadingConfig);
        }
        {
            // get env var FLEX_DATA_DIR
            if (System.getenv("FLEX_DATA_DIR") == null) {
                logger.info("FLEX_DATA_DIR is not set");
                return;
            }
            // The file will be uploaded to the server
            String personPath = "@" + System.getenv("FLEX_DATA_DIR") + "/person.csv";
            String knowsPath = "@" + System.getenv("FLEX_DATA_DIR") + "/person_knows_person.csv";
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
        assertOk(rep);
        jobId = rep.getValue().getJobId();
        logger.info("job id: " + jobId);
        waitJobFinished(jobId);
    }

    public void test3StartService() {
        Result<String> startServiceResponse =
                session.startService(new StartServiceRequest().graphId(graphId));
        if (startServiceResponse.isOk()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("start service success");
        } else {
            throw new RuntimeException(
                    "start service failed: " + startServiceResponse.getStatusMessage());
        }
    }

    public void test4CypherAdhocQuery() {
        String query = "MATCH(a) return COUNT(a);";
        org.neo4j.driver.Result result = neo4jSession.run(query);
        logger.info("result: " + result.toString());
    }

    public void test5GremlinAdhoQuery() {
        String query = "g.V().count();";
        try {
            List<org.apache.tinkerpop.gremlin.driver.Result> results =
                    gremlinClient.submit(query).all().get();
            logger.info("result: " + results.toString());
        } catch (Exception e) {
            e.printStackTrace();
            assert false;
        }
    }

    public void test6CreateCypherProcedure() {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("cypherProcedure");
        procedure.setDescription("a simple test procedure");
        procedure.setQuery("MATCH(p:person) where p.name=$personName RETURN p.id,p.age;");
        procedure.setType(CreateProcedureRequest.TypeEnum.CYPHER);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        assertOk(resp);
        cypherProcedureId = "cypherProcedure";
    }

    public void test7CreateCppProcedure1() {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("cppProcedure1");
        procedure.setDescription("a simple test procedure");
        // sampleAppFilePath is under the resources folder,with name sample_app.cc
        String sampleAppFilePath = "sample_app.cc";
        String sampleAppContent = "";
        try {
            sampleAppContent =
                    new String(
                            Files.readAllBytes(
                                    Paths.get(
                                            Thread.currentThread()
                                                    .getContextClassLoader()
                                                    .getResource(sampleAppFilePath)
                                                    .toURI())));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        if (sampleAppContent.isEmpty()) {
            throw new RuntimeException("sample app content is empty");
        }
        logger.info("sample app content: " + sampleAppContent);
        procedure.setQuery(sampleAppContent);
        procedure.setType(CreateProcedureRequest.TypeEnum.CPP);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        assertOk(resp);
        cppProcedureId1 = "cppProcedure1";
    }

    public void test7CreateCppProcedure2() {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("cppProcedure2");
        procedure.setDescription("a simple test procedure");
        // sampleAppFilePath is under the resources folder,with name sample_app.cc
        String sampleAppFilePath = "read_app_example.cc";
        String sampleAppContent = "";
        try {
            sampleAppContent =
                    new String(
                            Files.readAllBytes(
                                    Paths.get(
                                            Thread.currentThread()
                                                    .getContextClassLoader()
                                                    .getResource(sampleAppFilePath)
                                                    .toURI())));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        if (sampleAppContent.isEmpty()) {
            throw new RuntimeException("read_app_example content is empty");
        }
        logger.info("read_app_example content: " + sampleAppContent);
        procedure.setQuery(sampleAppContent);
        procedure.setType(CreateProcedureRequest.TypeEnum.CPP);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        assertOk(resp);
        cppProcedureId2 = "cppProcedure2";
    }

    public void test8Restart() {
        Result<String> resp = session.startService(new StartServiceRequest().graphId(graphId));
        assertOk(resp);
        // Sleep 10 seconds to wait for the service to restart
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("service restarted: " + resp.getValue());
    }

    public void test9GetGraphStatistics() {
        Result<GetGraphStatisticsResponse> resp = session.getGraphStatistics(graphId);
        assertOk(resp);
        logger.info("graph statistics: " + resp.getValue());
    }

    public void test9CallCppProcedureJson() {
        QueryRequest request = new QueryRequest();
        request.setQueryName(cppProcedureId1);
        request.addArgumentsItem(
                new TypedValue()
                        .value(1)
                        .type(
                                new GSDataType(
                                        new PrimitiveType()
                                                .primitiveType(
                                                        PrimitiveType.PrimitiveTypeEnum
                                                                .SIGNED_INT32))));
        Result<IrResult.CollectiveResults> resp = session.callProcedure(graphId, request);
        assertOk(resp);
    }

    public void test9CallCppProcedure1Current() {
        QueryRequest request = new QueryRequest();
        request.setQueryName(cppProcedureId1);
        request.addArgumentsItem(
                new TypedValue()
                        .value(1)
                        .type(
                                new GSDataType(
                                        new PrimitiveType()
                                                .primitiveType(
                                                        PrimitiveType.PrimitiveTypeEnum
                                                                .SIGNED_INT32))));
        Result<IrResult.CollectiveResults> resp = session.callProcedure(request);
        assertOk(resp);
    }

    public void test9CallCppProcedure2() {
        byte[] bytes = new byte[4 + 1];
        Encoder encoder = new Encoder(bytes);
        encoder.put_int(1);
        encoder.put_byte(
                (byte) 3); // Assume the procedure index is 3. since the procedures are sorted by
        // creation time.
        Result<byte[]> resp = session.callProcedureRaw(graphId, bytes);
        assertOk(resp);
    }

    public void test10CallCypherProcedureViaNeo4j() {
        String query = "CALL " + cypherProcedureId + "(\"" + personNameValue + "\") YIELD *;";
        logger.info("calling query: " + query);
        org.neo4j.driver.Result result = neo4jSession.run(query);
        logger.info("result: " + result.toString());
    }

    public void testQueryInterface() {
        String queryEndpoint =
                System.getProperty("interactive.procedure.endpoint", "http://localhost:10000");
        QueryInterface queryInterface = Driver.queryServiceOnly(queryEndpoint);
        byte[] bytes = new byte[4 + 1];
        Encoder encoder = new Encoder(bytes);
        encoder.put_int(1);
        encoder.put_byte((byte) 3); // Assume the procedure index is 3
        Result<byte[]> resp = queryInterface.callProcedureRaw(graphId, bytes);
        assertOk(resp);
    }

    public void test11CreateDriver() {
        // Create a new driver with all endpoints specified.
        // Assume the environment variables are set
        Driver driver = Driver.connect();
        Session session = driver.session();
    }

    private static <T> boolean assertOk(Result<T> result) {
        if (!result.isOk()) {
            Assert.fail("error: " + result.getStatusMessage());
        }
        return true;
    }

    private void waitJobFinished(String jobId) {
        if (jobId == null) {
            return;
        }
        while (true) {
            Result<JobStatus> rep = session.getJobStatus(jobId);
            assertOk(rep);
            JobStatus job = rep.getValue();
            if (job.getStatus() == JobStatus.StatusEnum.SUCCESS) {
                logger.info("job finished");
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
}
