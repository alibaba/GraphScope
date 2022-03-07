package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.SnapshotCache;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.EdgeId;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.operation.dml.*;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.EdgeKind;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import com.alibaba.maxgraph.common.util.PkHashUtils;
import com.alibaba.maxgraph.common.util.WriteSessionUtil;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.maxgraph.compiler.api.exception.PropertyDefNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            throw new MaxGraphException(e);
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
        EdgeId edgeId;
        EdgeKind edgeKind;
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        if (edgeTarget != null) {
            edgeId = edgeTarget.getEdgeId();
            edgeKind = edgeTarget.getEdgeKind();
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();
            String label = edgeRecordKey.getLabel();
            GraphElement edgeDef = schema.getElement(label);
            GraphElement srcVertexDef = schema.getElement(srcVertexRecordKey.getLabel());
            GraphElement dstVertexDef = schema.getElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals =
                    parseRawProperties(srcVertexDef, srcVertexRecordKey.getProperties());
            long srcVertexHashId =
                    getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals =
                    parseRawProperties(dstVertexDef, dstVertexRecordKey.getProperties());
            long dstVertexHashId =
                    getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = edgeRecordKey.getEdgeInnerId();
            edgeId =
                    new EdgeId(
                            new VertexId(srcVertexHashId),
                            new VertexId(dstVertexHashId),
                            edgeInnerId);
            edgeKind =
                    EdgeKind.newBuilder()
                            .setEdgeLabelId(new LabelId(labelId))
                            .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                            .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                            .build();
        }
        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, true));
        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, false));
    }

    private void addUpdateEdgeOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId;
        EdgeKind edgeKind;
        GraphElement edgeDef;
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        Map<String, Object> properties = dataRecord.getProperties();
        if (edgeTarget != null) {
            edgeId = edgeTarget.getEdgeId();
            edgeKind = edgeTarget.getEdgeKind();
            edgeDef = schema.getElement(edgeKind.getEdgeLabelId().getId());
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();
            String label = edgeRecordKey.getLabel();
            edgeDef = schema.getElement(label);
            GraphElement srcVertexDef = schema.getElement(srcVertexRecordKey.getLabel());
            GraphElement dstVertexDef = schema.getElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals =
                    parseRawProperties(srcVertexDef, srcVertexRecordKey.getProperties());
            long srcVertexHashId =
                    getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals =
                    parseRawProperties(dstVertexDef, dstVertexRecordKey.getProperties());
            long dstVertexHashId =
                    getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = edgeRecordKey.getEdgeInnerId();
            edgeId =
                    new EdgeId(
                            new VertexId(srcVertexHashId),
                            new VertexId(dstVertexHashId),
                            edgeInnerId);
            edgeKind =
                    EdgeKind.newBuilder()
                            .setEdgeLabelId(new LabelId(labelId))
                            .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                            .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                            .build();
        }
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(edgeDef, properties);
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, true));
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, false));
    }

    private void addOverwriteEdgeOperation(
            OperationBatch.Builder batchBuilder, GraphSchema schema, DataRecord dataRecord) {
        EdgeId edgeId;
        EdgeKind edgeKind;
        GraphElement edgeDef;
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        Map<String, Object> properties = dataRecord.getProperties();
        if (edgeTarget != null) {
            edgeId = edgeTarget.getEdgeId();
            edgeKind = edgeTarget.getEdgeKind();
            edgeDef = schema.getElement(edgeKind.getEdgeLabelId().getId());
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();
            String label = edgeRecordKey.getLabel();
            edgeDef = schema.getElement(label);
            GraphElement srcVertexDef = schema.getElement(srcVertexRecordKey.getLabel());
            GraphElement dstVertexDef = schema.getElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals =
                    parseRawProperties(srcVertexDef, srcVertexRecordKey.getProperties());
            long srcVertexHashId =
                    getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals =
                    parseRawProperties(dstVertexDef, dstVertexRecordKey.getProperties());
            long dstVertexHashId =
                    getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = this.edgeIdGenerator.getNextId();
            edgeId =
                    new EdgeId(
                            new VertexId(srcVertexHashId),
                            new VertexId(dstVertexHashId),
                            edgeInnerId);
            edgeKind =
                    EdgeKind.newBuilder()
                            .setEdgeLabelId(new LabelId(labelId))
                            .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                            .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                            .build();
        }
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
        long hashId = getHashId(labelId, pkVals, vertexDef);
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
        long hashId = getHashId(labelId, pkVals, vertexDef);
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        batchBuilder.addOperation(
                new UpdateVertexOperation(
                        new VertexId(hashId), new LabelId(labelId), propertyVals));
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
        long hashId = getHashId(labelId, propertyVals, vertexDef);
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

    public static long getHashId(
            int labelId, Map<Integer, PropertyValue> pkVals, GraphElement graphElement) {
        List<Integer> pkIdxs = graphElement.getPkPropertyIndices();
        List<GraphProperty> propertyDefs = graphElement.getPropertyList();
        List<byte[]> pks = new ArrayList<>(pkIdxs.size());
        for (int pkIdx : pkIdxs) {
            int propertyId = propertyDefs.get(pkIdx).getId();
            byte[] valBytes = pkVals.get(propertyId).getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
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
