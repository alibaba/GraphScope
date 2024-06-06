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
import com.alibaba.graphscope.interactive.openapi.model.*;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@TestMethodOrder(OrderAnnotation.class)
public class DriverTest {

    public static class Encoder {
        public Encoder(byte[] bs) {
            this.bs = bs;
            this.loc = 0;
        }

        public static int serialize_long(byte[] bytes, int offset, long value) {
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            return offset;
        }

        public static int serialize_double(byte[] bytes, int offset, double value) {
            long long_value = Double.doubleToRawLongBits(value);
            return serialize_long(bytes, offset, long_value);
        }

        public static int serialize_int(byte[] bytes, int offset, int value) {
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            value >>= 8;
            bytes[offset++] = (byte) (value & 0xFF);
            return offset;
        }

        public static int serialize_byte(byte[] bytes, int offset, byte value) {
            bytes[offset++] = value;
            return offset;
        }

        public static int serialize_bytes(byte[] bytes, int offset, byte[] value) {
            offset = serialize_int(bytes, offset, value.length);
            System.arraycopy(value, 0, bytes, offset, value.length);
            return offset + value.length;
        }

        public void put_int(int value) {
            this.loc = serialize_int(this.bs, this.loc, value);
        }

        public void put_byte(byte value) {
            this.loc = serialize_byte(this.bs, this.loc, value);
        }

        public void put_long(long value) {
            this.loc = serialize_long(this.bs, this.loc, value);
        }

        public void put_double(double value) {
            this.loc = serialize_double(this.bs, this.loc, value);
        }

        public void put_bytes(byte[] bytes) {
            this.loc = serialize_bytes(this.bs, this.loc, bytes);
        }

        byte[] bs;
        int loc;
    }

    static final class Decoder {
        public Decoder(byte[] bs) {
            this.bs = bs;
            this.loc = 0;
            this.len = this.bs.length;
        }

        public static int get_int(byte[] bs, int loc) {
            int ret = (bs[loc + 3] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 2] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 1] & 0xff);
            ret <<= 8;
            ret |= (bs[loc] & 0xff);
            return ret;
        }

        public static long get_long(byte[] bs, int loc) {
            long ret = (bs[loc + 7] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 6] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 5] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 4] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 3] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 2] & 0xff);
            ret <<= 8;
            ret |= (bs[loc + 1] & 0xff);
            ret <<= 8;
            ret |= (bs[loc] & 0xff);
            return ret;
        }

        public long get_long() {
            long ret = get_long(this.bs, this.loc);
            this.loc += 8;
            return ret;
        }

        public int get_int() {
            int ret = get_int(this.bs, this.loc);
            this.loc += 4;
            return ret;
        }

        public byte get_byte() {
            return (byte) (bs[loc++] & 0xFF);
        }

        public String get_string() {
            int strlen = this.get_int();
            String ret = new String(this.bs, this.loc, strlen);
            this.loc += strlen;
            return ret;
        }

        public boolean empty() {
            return loc == len;
        }

        byte[] bs;
        int loc;
        int len;
    }

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
    public void test6CreateCypherProcedure() {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("cypherProcedure");
        procedure.setDescription("a simple test procedure");
        procedure.setQuery("MATCH(p:person) RETURN COUNT(p);");
        procedure.setType(CreateProcedureRequest.TypeEnum.CYPHER);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        assertOk(resp);
        cypherProcedureId = "cypherProcedure";
    }

    @Test
    @Order(8)
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

    @Test
    @Order(9)
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

    @Test
    @Order(10)
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

    @Test
    @Order(11)
    public void test9CallCppProcedure1() {
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

    @Test
    @Order(12)
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

    @Test
    @Order(13)
    public void test9CallCppProcedure2() {
        byte[] bytes = new byte[4 + 1];
        Encoder encoder = new Encoder(bytes);
        encoder.put_int(1);
        encoder.put_byte((byte) 1); // Assume the procedure index is 1
        Result<byte[]> resp = session.callProcedureRaw(graphId, bytes);
        assertOk(resp);
    }

    @Test
    @Order(14)
    public void test10CallCypherProcedureViaNeo4j() {
        String query = "CALL " + cypherProcedureId + "() YIELD *;";
        org.neo4j.driver.Result result = neo4jSession.run(query);
        logger.info("result: " + result.toString());
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

    private static <T> boolean assertOk(Result<T> result) {
        if (!result.isOk()) {
            System.out.println(result.getStatusMessage());
            return false;
        }
        return true;
    }
}
