package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.common.exception.PropertyDefNotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.GraphElement;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.alibaba.graphscope.groot.common.schema.wrapper.EdgeKind;
import com.alibaba.graphscope.groot.common.schema.wrapper.LabelId;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
import com.alibaba.graphscope.groot.common.util.EdgeRecordKey;
import com.alibaba.graphscope.groot.common.util.PkHashUtils;
import com.alibaba.graphscope.groot.common.util.VertexRecordKey;
import com.alibaba.graphscope.groot.common.util.WriteSessionUtil;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.operation.dml.*;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GraphWriter implements MetricsAgent {

    public static final String WRITE_REQUESTS_TOTAL = "write.requests.total";
    public static final String WRITE_REQUESTS_PER_SECOND = "write.requests.per.second";
    public static final String INGESTOR_BLOCK_TIME_MS = "ingestor.block.time.ms";
    public static final String INGESTOR_BLOCK_TIME_AVG_MS = "ingestor.block.time.avg.ms";
    public static final String PENDING_WRITE_COUNT = "pending.write.count";

    private AtomicLong writeRequestsTotal;
    private volatile long lastUpdateWriteRequestsTotal;
    private volatile long writeRequestsPerSecond;
    private volatile long lastUpdateTime;
    private AtomicLong ingestorBlockTimeNano;
    private volatile long ingestorBlockTimeAvgMs;
    private volatile long lastUpdateIngestorBlockTimeNano;
    private AtomicInteger pendingWriteCount;

    private SnapshotCache snapshotCache;
    private EdgeIdGenerator edgeIdGenerator;
    private MetaService metaService;
    private RoleClients<IngestorWriteClient> ingestWriteClients;
    private AtomicLong lastWrittenSnapshotId = new AtomicLong(0L);

    public GraphWriter(
            SnapshotCache snapshotCache,
            EdgeIdGenerator edgeIdGenerator,
            MetaService metaService,
            RoleClients<IngestorWriteClient> ingestWriteClients,
            MetricsCollector metricsCollector) {
        this.snapshotCache = snapshotCache;
        this.edgeIdGenerator = edgeIdGenerator;
        this.metaService = metaService;
        this.ingestWriteClients = ingestWriteClients;
        initMetrics();
        metricsCollector.register(this, () -> updateMetrics());
    }

    public long writeBatch(
            String requestId, String writeSession, List<WriteRequest> writeRequests) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        writeBatch(
                requestId,
                writeSession,
                writeRequests,
                new CompletionCallback<Long>() {
                    @Override
                    public void onCompleted(Long res) {
                        future.complete(res);
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new GrootException(e);
        }
    }

    public void writeBatch(
            String requestId,
            String writeSession,
            List<WriteRequest> writeRequests,
            CompletionCallback<Long> callback) {
        this.pendingWriteCount.incrementAndGet();
        GraphSchema schema = snapshotCache.getSnapshotWithSchema().getGraphDef();
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
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
                    throw new IllegalArgumentException(
                            "Invalid operationType [" + operationType + "]");
            }
        }
        OperationBatch operationBatch = batchBuilder.build();
        int writeQueueId = getWriteQueueId(writeSession);
        int ingestorId = this.metaService.getIngestorIdForQueue(writeQueueId);
        long startTimeNano = System.nanoTime();
        this.ingestWriteClients
                .getClient(ingestorId)
                .writeIngestorAsync(
                        requestId,
                        writeQueueId,
                        operationBatch,
                        new CompletionCallback<Long>() {
                            @Override
                            public void onCompleted(Long res) {
                                long writeSnapshotId = res;
                                lastWrittenSnapshotId.updateAndGet(
                                        x -> x < writeSnapshotId ? writeSnapshotId : x);
                                writeRequestsTotal.addAndGet(writeRequests.size());
                                finish();
                                callback.onCompleted(res);
                            }

                            @Override
                            public void onError(Throwable t) {
                                finish();
                                callback.onError(t);
                            }

                            void finish() {
                                long ingestorCompleteTimeNano = System.nanoTime();
                                ingestorBlockTimeNano.addAndGet(
                                        ingestorCompleteTimeNano - startTimeNano);
                                pendingWriteCount.decrementAndGet();
                            }
                        });
    }

    public boolean flushSnapshot(long snapshotId, long waitTimeMs) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        this.snapshotCache.addListener(snapshotId, () -> latch.countDown());
        return latch.await(waitTimeMs, TimeUnit.MILLISECONDS);
    }

    public boolean flushLastSnapshot(long waitTimeMs) throws InterruptedException {
        long snapshotId = this.lastWrittenSnapshotId.get();
        return this.flushSnapshot(snapshotId, waitTimeMs);
    }

    private int getWriteQueueId(String session) {
        int queueCount = this.metaService.getQueueCount();
        if (queueCount <= 1) {
            throw new IllegalStateException("expect queueCount > 1, but was [" + queueCount + "]");
        }
        long clientIdx = WriteSessionUtil.getClientIdx(session);
        return (int) (clientIdx % (queueCount - 1)) + 1;
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
        long hashId = getPrimaryKeysHashId(labelId, pkVals, vertexDef);
        batchBuilder.addOperation(
                new DeleteVertexOperation(new VertexId(hashId), new LabelId(labelId)));
    }

    private void addUpdateVertexOperation(
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
        long hashId = getPrimaryKeysHashId(labelId, pkVals, vertexDef);
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
                            throw new PropertyDefNotFoundException(
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
            long edgeInnerId =
                    overwrite ? edgeIdGenerator.getNextId() : edgeRecordKey.getEdgeInnerId();

            return new EdgeId(
                    new VertexId(srcVertexHashId), new VertexId(dstVertexHashId), edgeInnerId);
        }
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
        List<GraphProperty> pklist = graphElement.getPrimaryKeyList();
        List<byte[]> pks = new ArrayList<>(pklist.size());
        for (GraphProperty pk : pklist) {
            byte[] valBytes = properties.get(pk.getId()).getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
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

    @Override
    public void initMetrics() {
        this.lastUpdateTime = System.nanoTime();
        this.writeRequestsTotal = new AtomicLong(0L);
        this.writeRequestsPerSecond = 0L;
        this.ingestorBlockTimeNano = new AtomicLong(0L);
        this.lastUpdateIngestorBlockTimeNano = 0L;
        this.pendingWriteCount = new AtomicInteger(0);
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<String, String>() {
            {
                put(WRITE_REQUESTS_TOTAL, String.valueOf(writeRequestsTotal.get()));
                put(WRITE_REQUESTS_PER_SECOND, String.valueOf(writeRequestsPerSecond));
                put(INGESTOR_BLOCK_TIME_MS, String.valueOf(ingestorBlockTimeNano.get() / 1000000));
                put(INGESTOR_BLOCK_TIME_AVG_MS, String.valueOf(ingestorBlockTimeAvgMs));
                put(PENDING_WRITE_COUNT, String.valueOf(pendingWriteCount.get()));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
            WRITE_REQUESTS_TOTAL,
            WRITE_REQUESTS_PER_SECOND,
            INGESTOR_BLOCK_TIME_MS,
            INGESTOR_BLOCK_TIME_AVG_MS,
            PENDING_WRITE_COUNT,
        };
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long writeRequests = this.writeRequestsTotal.get();
        long ingestBlockTime = this.ingestorBlockTimeNano.get();

        long interval = currentTime - this.lastUpdateTime;
        this.writeRequestsPerSecond =
                1000000000 * (writeRequests - this.lastUpdateWriteRequestsTotal) / interval;
        this.ingestorBlockTimeAvgMs =
                1000 * (ingestBlockTime - this.lastUpdateIngestorBlockTimeNano) / interval;

        this.lastUpdateWriteRequestsTotal = writeRequests;
        this.lastUpdateIngestorBlockTimeNano = ingestBlockTime;
        this.lastUpdateTime = currentTime;
    }
}
