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

import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.openapi.model.*;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@TestMethodOrder(OrderAnnotation.class)
public class DriverTest {
    private static final Logger logger = Logger.getLogger(DriverTest.class.getName());

    private static Driver driver;
    private static Session session;
    private static org.neo4j.driver.Session neo4jSession;
    private static Client gremlinClient;
    private static String graphId;
    private static String jobId;
    private static String procedureId;

    @BeforeAll
    public static void beforeClass() {
        String interactiveEndpoint = System.getProperty("interactive.endpoint", "localhost:7777");
        driver = Driver.connect(interactiveEndpoint);
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

    @Test
    @Order(1)
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

    @Test
    @Order(2)
    public void test1BulkLoading() {
        SchemaMapping schemaMapping = new SchemaMapping();
        schemaMapping.setGraph(graphId);
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
    }

    @Test
    @Order(3)
    public void test2waitJobFinished() {
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

    @Test
    @Order(4)
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

    @Test
    @Order(5)
    public void test4CypherAdhocQuery() {
        String query = "MATCH(a) return COUNT(a);";
        org.neo4j.driver.Result result = neo4jSession.run(query);
        logger.info("result: " + result.toString());
    }

    @Test
    @Order(6)
    public void test5GremlinAdhoQuery() throws Exception {
        String query = "g.V().count();";
        List<org.apache.tinkerpop.gremlin.driver.Result> results =
                gremlinClient.submit(query).all().get();
        logger.info("result: " + results.toString());
    }

    @Test
    @Order(7)
    public void test6CreateProcedure() {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("testProcedure");
        procedure.setDescription("a simple test procedure");
        procedure.setQuery("MATCH(p:person) RETURN COUNT(p);");
        procedure.setType(CreateProcedureRequest.TypeEnum.CYPHER);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        assertOk(resp);
        procedureId = "testProcedure";
    }

    @Test
    @Order(8)
    public void test7Restart() {
        Result<String> resp = session.restartService();
        assertOk(resp);
        // Sleep 5 seconds to wait for the service to restart
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("service restarted: " + resp.getValue());
    }

    @Test
    @Order(9)
    public void test8CallProcedureViaNeo4j() {
        org.neo4j.driver.Result result = neo4jSession.run("CALL testProcedure() YIELD *;");
        logger.info("result: " + result.toString());
    }

    @AfterAll
    public static void afterClass() {
        logger.info("clean up");
        if (graphId != null) {
            if (procedureId != null) {
                Result<String> resp = session.deleteProcedure(graphId, procedureId);
                logger.info("procedure deleted: " + resp.getValue());
            }
            Result<String> resp = session.deleteGraph(graphId);
            assertOk(resp);
            logger.info("graph deleted: " + resp.getValue());
        }
    }

    private static <T> boolean assertOk(Result<T> result) {
        if (!result.isOk()) {
            System.out.println(result.getStatusMessage());
            return false;
        }
        return true;
    }
}
