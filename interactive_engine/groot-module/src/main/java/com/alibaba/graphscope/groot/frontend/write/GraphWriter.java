package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.exception.NotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.GraphElement;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.alibaba.graphscope.groot.common.schema.wrapper.EdgeKind;
import com.alibaba.graphscope.groot.common.schema.wrapper.LabelId;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
import com.alibaba.graphscope.groot.common.util.*;
import com.alibaba.graphscope.groot.frontend.SnapshotCache;
import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.operation.dml.*;
import com.alibaba.graphscope.proto.groot.RequestOptionsPb;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class GraphWriter {
    private static final Logger logger = LoggerFactory.getLogger(GraphWriter.class);

    private LongCounter writeCounter;
    private LongHistogram writeHistogram;
    private final SnapshotCache snapshotCache;
    private final EdgeIdGenerator edgeIdGenerator;
    private final AtomicLong lastWrittenSnapshotId = new AtomicLong(0L);

    private final KafkaAppender kafkaAppender;
    private ScheduledExecutorService scheduler;

    public GraphWriter(
            SnapshotCache snapshotCache,
            EdgeIdGenerator edgeIdGenerator,
            KafkaAppender appender,
            Configs configs) {
        this.snapshotCache = snapshotCache;
        this.edgeIdGenerator = edgeIdGenerator;
        initMetrics();
        this.kafkaAppender = appender;
    }

    public void start() {
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "kafka-appender-try-start", logger));

        this.scheduler.scheduleWithFixedDelay(
                this::tryStartProcessors, 0, 2000, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (this.scheduler != null) {
            this.scheduler.shutdown();
            try {
                this.scheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.scheduler = null;
        }
        kafkaAppender.stop();
    }

    private void tryStartProcessors() {
        if (!kafkaAppender.isStarted()) {
            kafkaAppender.start();
        }
    }

    public void writeBatch(
            String requestId,
            String writeSession,
            List<WriteRequest> writeRequests,
            RequestOptionsPb optionsPb,
            CompletionCallback<Long> callback) {
        GraphSchema schema = snapshotCache.getSnapshotWithSchema().getGraphDef();
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
        String upTraceId = optionsPb == null ? null : optionsPb.getTraceId();
        for (WriteRequest writeRequest : writeRequests) {
            OperationType operationType = writeRequest.getOperationType();
            DataRecord dataRecord = writeRequest.getDataRecord();
            switch (operationType) {
                case OVERWRITE_VERTEX:
                    addOverwriteVertexOperation(batchBuilder, schema, dataRecord);
                    break;
                case UPDATE_VERTEX:
                    addUpdateVertexOperation(batchBuilder, schema, dataRecord);
                    break;
                case DELETE_VERTEX:
                    addDeleteVertexOperation(batchBuilder, schema, dataRecord);
                    break;
                case OVERWRITE_EDGE:
                    addOverwriteEdgeOperation(batchBuilder, schema, dataRecord);
                    break;
                case UPDATE_EDGE:
                    addUpdateEdgeOperation(batchBuilder, schema, dataRecord);
                    break;
                case DELETE_EDGE:
                    addDeleteEdgeOperation(batchBuilder, schema, dataRecord);
                    break;
                case CLEAR_VERTEX_PROPERTIES:
                    addClearVertexPropertiesOperation(batchBuilder, schema, dataRecord);
                    break;
                case CLEAR_EDGE_PROPERTIES:
                    addClearEdgePropertiesOperation(batchBuilder, schema, dataRecord);
                    break;
                default:
                    throw new InvalidArgumentException(
                            "Invalid operationType [" + operationType + "]");
            }
        }
        batchBuilder.setTraceId(upTraceId);
        OperationBatch operationBatch = batchBuilder.build();
        long startTime = System.currentTimeMillis();
        AttributesBuilder attrs = Attributes.builder();
        this.kafkaAppender.ingestBatch(
                requestId,
                operationBatch,
                new IngestCallback() {
                    @Override
                    public void onSuccess(long snapshotId) {
                        attrs.put("success", true).put("message", "");
                        writeCounter.add(writeRequests.size(), attrs.build());
                        writeHistogram.record(
                                System.currentTimeMillis() - startTime, attrs.build());
                        lastWrittenSnapshotId.updateAndGet(x -> Math.max(x, snapshotId));
                        callback.onCompleted(snapshotId);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        attrs.put("success", false).put("message", e.getMessage());
                        writeCounter.add(writeRequests.size(), attrs.build());
                        writeHistogram.record(
                                System.currentTimeMillis() - startTime, attrs.build());
                        callback.onError(e);
                    }
                });
    }

    public List<Long> replayWALFrom(long offset, long timestamp) throws IOException {
        return kafkaAppender.replayDMLRecordsFrom(offset, timestamp);
    }

    public boolean flushSnapshot(long snapshotId, long waitTimeMs) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.snapshotCache.addListener(snapshotId, latch::countDown);
        return latch.await(waitTimeMs, TimeUnit.MILLISECONDS);
    }

    public boolean flushLastSnapshot(long waitTimeMs) throws InterruptedException {
        long snapshotId = this.lastWrittenSnapshotId.get();
        return this.flushSnapshot(snapshotId, waitTimeMs);
    }

    private void addDeleteEdgeOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId = getEdgeId(schema, dataRecord, false);
        EdgeKind edgeKind = getEdgeKind(schema, dataRecord);

        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, true));
        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, false));
    }

    private void addUpdateEdgeOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId = getEdgeId(schema, dataRecord, false);
        if (edgeId.id == 0) {
            // This is for update edge, if edgeInnerId is 0, generate new id, incase there isn't
            // such an edge
            edgeId.id = edgeIdGenerator.getNextId();
        }
        EdgeKind edgeKind = getEdgeKind(schema, dataRecord);
        GraphElement edgeDef = schema.getElement(edgeKind.getEdgeLabelId().getId());

        Map<String, Object> properties = dataRecord.getProperties();

        Map<Integer, PropertyValue> propertyVals = parseRawProperties(edgeDef, properties);
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, true));
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, false));
    }

    private void addClearEdgePropertiesOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId = getEdgeId(schema, dataRecord, false);
        EdgeKind edgeKind = getEdgeKind(schema, dataRecord);
        GraphElement edgeDef = schema.getElement(edgeKind.getEdgeLabelId().getId());
        List<Integer> propertyIds = getNonPrimaryKeyIds(edgeDef, dataRecord.getProperties());
        batchBuilder.addOperation(
                new ClearEdgePropertyOperation(edgeId, edgeKind, propertyIds, true));
        batchBuilder.addOperation(
                new ClearEdgePropertyOperation(edgeId, edgeKind, propertyIds, false));
    }

    private void addOverwriteEdgeOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId = getEdgeId(schema, dataRecord, true);
        EdgeKind edgeKind = getEdgeKind(schema, dataRecord);
        GraphElement edgeDef = schema.getElement(edgeKind.getEdgeLabelId().getId());

        Map<String, Object> properties = dataRecord.getProperties();

        Map<Integer, PropertyValue> propertyVals = parseRawProperties(edgeDef, properties);
        batchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, propertyVals, true));
        batchBuilder.addOperation(
                new OverwriteEdgeOperation(edgeId, edgeKind, propertyVals, false));
    }

    private void addDeleteVertexOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        String label = vertexRecordKey.getLabel();
        GraphElement vertexDef = schema.getElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals =
                parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        Map<String, Object> properties = dataRecord.getProperties();
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        long hashId = getPrimaryKeysHashId(labelId, propertyVals, vertexDef);
        batchBuilder.addOperation(
                new DeleteVertexOperation(new VertexId(hashId), new LabelId(labelId)));
    }

    private void addUpdateVertexOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        String label = vertexRecordKey.getLabel();
        GraphElement vertexDef = schema.getElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals =
                parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        Map<String, Object> properties = dataRecord.getProperties();
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        long hashId = getPrimaryKeysHashId(labelId, propertyVals, vertexDef);
        batchBuilder.addOperation(
                new UpdateVertexOperation(
                        new VertexId(hashId), new LabelId(labelId), propertyVals));
    }

    private void addClearVertexPropertiesOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        String label = vertexRecordKey.getLabel();
        GraphElement vertexDef = schema.getElement(label);
        int labelId = vertexDef.getLabelId();
        Map<String, Object> pkProperties = vertexRecordKey.getProperties();
        Map<String, Object> properties = dataRecord.getProperties();
        Map<String, Object> allProperties = new HashMap<>();
        allProperties.putAll(pkProperties);
        allProperties.putAll(properties);

        long hashId = getPrimaryKeysHashIdFromRaw(labelId, allProperties, vertexDef);
        List<Integer> propertyIds = getNonPrimaryKeyIds(vertexDef, allProperties);
        batchBuilder.addOperation(
                new ClearVertexPropertyOperation(
                        new VertexId(hashId), new LabelId(labelId), propertyIds));
    }

    private void addOverwriteVertexOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        Map<String, Object> properties = dataRecord.getProperties();
        String label = vertexRecordKey.getLabel();
        GraphElement vertexDef = schema.getElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals =
                parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        long hashId = getPrimaryKeysHashId(labelId, propertyVals, vertexDef);
        batchBuilder.addOperation(
                new OverwriteVertexOperation(
                        new VertexId(hashId), new LabelId(labelId), propertyVals));
    }

    public static Map<Integer, PropertyValue> parseRawProperties(
            GraphElement graphElement, Map<String, Object> properties) {
        Map<Integer, PropertyValue> res = new HashMap<>();
        if (properties != null) {
            properties.forEach(
                    (propertyName, valString) -> {
                        GraphProperty propertyDef = graphElement.getProperty(propertyName);
                        if (propertyDef == null) {
                            throw new NotFoundException(
                                    "property ["
                                            + propertyName
                                            + "] not found in ["
                                            + graphElement.getLabel()
                                            + "]");
                        }
                        int id = propertyDef.getId();
                        DataType dataType = propertyDef.getDataType();
                        PropertyValue propertyValue = new PropertyValue(dataType, valString);
                        res.put(id, propertyValue);
                    });
        }
        return res;
    }

    private EdgeId getEdgeId(GraphSchema schema, DataRecord dataRecord, boolean overwrite) {
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        if (edgeTarget != null) {
            return edgeTarget.getEdgeId();
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();

            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();

            GraphElement srcVertexDef = schema.getElement(srcVertexRecordKey.getLabel());
            GraphElement dstVertexDef = schema.getElement(dstVertexRecordKey.getLabel());
            Map<Integer, PropertyValue> srcVertexPkVals =
                    parseRawProperties(srcVertexDef, srcVertexRecordKey.getProperties());
            long srcVertexHashId =
                    getPrimaryKeysHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals =
                    parseRawProperties(dstVertexDef, dstVertexRecordKey.getProperties());
            long dstVertexHashId =
                    getPrimaryKeysHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            //            long edgeInnerId =
            //                    overwrite ? edgeIdGenerator.getNextId() :
            // edgeRecordKey.getEdgeInnerId();
            long edgeInnerId =
                    getEdgeInnerId(
                            srcVertexHashId,
                            dstVertexHashId,
                            overwrite,
                            edgeRecordKey,
                            schema,
                            dataRecord);
            return new EdgeId(
                    new VertexId(srcVertexHashId), new VertexId(dstVertexHashId), edgeInnerId);
        }
    }

    /**
     * if enableHashEid == true: when eid == 0(client input none eid), return hash eid as final eid
     * if enableHashEid == false: when eid == 0(client input none eid), return 0
     * @param srcId srcVertexId
     * @param dstId dstVertexId
     * @param overwrite if insert
     * @param edgeRecordKey edgeRecordKey
     * @param schema GraphSchema
     * @param dataRecord DataRecord
     * @return eid
     */
    private long getEdgeInnerId(
            long srcId,
            long dstId,
            boolean overwrite,
            EdgeRecordKey edgeRecordKey,
            GraphSchema schema,
            DataRecord dataRecord) {
        long edgeInnerId;
        GraphElement edgeDef = schema.getElement(edgeRecordKey.getLabel());
        List<GraphProperty> pks = edgeDef.getPrimaryKeyList();
        if (pks != null && pks.size() > 0) {
            Map<Integer, PropertyValue> edgePkVals =
                    parseRawProperties(edgeDef, dataRecord.getProperties());
            List<byte[]> edgePkBytes = getPkBytes(edgePkVals, edgeDef);
            int edgeLabelId = edgeDef.getLabelId();
            long eid = edgeIdGenerator.getHashId(srcId, dstId, edgeLabelId, edgePkBytes);
            edgeInnerId =
                    overwrite
                            ? eid
                            : (edgeRecordKey.getEdgeInnerId() == 0
                                    ? eid
                                    : edgeRecordKey.getEdgeInnerId());
        } else {
            edgeInnerId = overwrite ? edgeIdGenerator.getNextId() : edgeRecordKey.getEdgeInnerId();
        }
        return edgeInnerId;
    }

    private EdgeKind getEdgeKind(GraphSchema schema, DataRecord dataRecord) {
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        if (edgeTarget != null) {
            return edgeTarget.getEdgeKind();
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            String label = edgeRecordKey.getLabel();
            GraphElement edgeDef = schema.getElement(label);

            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();

            GraphElement srcVertexDef = schema.getElement(srcVertexRecordKey.getLabel());
            GraphElement dstVertexDef = schema.getElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();

            return EdgeKind.newBuilder()
                    .setEdgeLabelId(new LabelId(labelId))
                    .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                    .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                    .build();
        }
    }

    public static long getPrimaryKeysHashId(
            int labelId, Map<Integer, PropertyValue> properties, GraphElement graphElement) {
        return PkHashUtils.hash(labelId, getPkBytes(properties, graphElement));
    }

    public static List<byte[]> getPkBytes(
            Map<Integer, PropertyValue> properties, GraphElement graphElement) {
        List<GraphProperty> pklist = graphElement.getPrimaryKeyList();
        List<byte[]> pks = new ArrayList<>(pklist.size());
        for (GraphProperty pk : pklist) {
            byte[] valBytes = properties.get(pk.getId()).getValBytes();
            pks.add(valBytes);
        }
        return pks;
    }

    public static long getPrimaryKeysHashIdFromRaw(
            int labelId, Map<String, Object> properties, GraphElement graphElement) {
        List<String> pklist = graphElement.getPrimaryKeyNameList();
        List<byte[]> pks = new ArrayList<>(pklist.size());

        for (String propertyName : pklist) {
            GraphProperty propertyDef = graphElement.getProperty(propertyName);
            Object valString = properties.get(propertyName);
            DataType dataType = propertyDef.getDataType();
            PropertyValue propertyValue = new PropertyValue(dataType, valString);
            byte[] valBytes = propertyValue.getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
    }

    public static List<Integer> getNonPrimaryKeyIds(
            GraphElement graphElement, Map<String, Object> properties) {
        List<String> pklist = graphElement.getPrimaryKeyNameList();
        List<Integer> ids = new ArrayList<>();
        properties.forEach(
                (name, val) -> {
                    if (pklist == null || !pklist.contains(name)) {
                        int id = graphElement.getProperty(name).getId();
                        ids.add(id);
                    }
                });
        return ids;
    }

    public void initMetrics() {
        Meter meter = GlobalOpenTelemetry.getMeter("GraphWriter");
        this.writeCounter =
                meter.counterBuilder("groot.frontend.write.count")
                        .setDescription("Total count of write requests of one store node.")
                        .build();
        this.writeHistogram =
                meter.histogramBuilder("groot.frontend.write.duration")
                        .setDescription("Duration of write requests that be persist into the disk.")
                        .ofLongs()
                        .setUnit("ms")
                        .build();
    }
}
