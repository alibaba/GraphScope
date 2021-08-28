package com.alibaba.maxgraph.v2.frontend.write;


import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.exception.PropertyDefNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.operation.*;
import com.alibaba.maxgraph.v2.common.operation.dml.*;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.common.util.PkHashUtils;
import com.alibaba.maxgraph.v2.frontend.IngestorWriteClient;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;
import com.alibaba.maxgraph.v2.frontend.utils.WriteSessionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GraphWriter {

    private SnapshotCache snapshotCache;
    private EdgeIdGenerator edgeIdGenerator;
    private MetaService metaService;
    private RoleClients<IngestorWriteClient> ingestWriteClients;
    private AtomicLong lastWrittenSnapshotId = new AtomicLong(0L);

    public GraphWriter(SnapshotCache snapshotCache, EdgeIdGenerator edgeIdGenerator, MetaService metaService,
                       RoleClients<IngestorWriteClient> ingestWriteClients) {
        this.snapshotCache = snapshotCache;
        this.edgeIdGenerator = edgeIdGenerator;
        this.metaService = metaService;
        this.ingestWriteClients = ingestWriteClients;
    }

    public long writeBatch(String requestId, String writeSession, List<WriteRequest> writeRequests) {
        GraphSchema schema = snapshotCache.fetchSchema().getSchema();
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
                    throw new IllegalArgumentException("Invalid operationType [" + operationType + "]");
            }
        }
        OperationBatch operationBatch = batchBuilder.build();
        int writeQueueId = getWriteQueueId(writeSession);
        int ingestorId = this.metaService.getIngestorIdForQueue(writeQueueId);
        BatchId batchId = this.ingestWriteClients.getClient(ingestorId).writeIngestor(requestId, writeQueueId,
                operationBatch);
        long writeSnapshotId = batchId.getSnapshotId();
        this.lastWrittenSnapshotId.updateAndGet(x -> x < writeSnapshotId ? writeSnapshotId : x);
        return writeSnapshotId;
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

    private void addDeleteEdgeOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                        DataRecord dataRecord) {
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
            SchemaElement edgeDef = schema.getSchemaElement(label);
            SchemaElement srcVertexDef = schema.getSchemaElement(srcVertexRecordKey.getLabel());
            SchemaElement dstVertexDef = schema.getSchemaElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals = parseRawProperties(srcVertexDef,
                    srcVertexRecordKey.getProperties());
            long srcVertexHashId = getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals = parseRawProperties(dstVertexDef,
                    dstVertexRecordKey.getProperties());
            long dstVertexHashId = getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = edgeRecordKey.getEdgeInnerId();
            edgeId = new EdgeId(new VertexId(srcVertexHashId), new VertexId(dstVertexHashId), edgeInnerId);
            edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(new LabelId(labelId))
                    .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                    .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                    .build();
        }
        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, true));
        batchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, false));
    }

    private void addUpdateEdgeOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                        DataRecord dataRecord) {
        EdgeId edgeId;
        EdgeKind edgeKind;
        SchemaElement edgeDef;
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        Map<String, Object> properties = dataRecord.getProperties();
        if (edgeTarget != null) {
            edgeId = edgeTarget.getEdgeId();
            edgeKind = edgeTarget.getEdgeKind();
            edgeDef = schema.getSchemaElement(edgeKind.getEdgeLabelId().getId());
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();
            String label = edgeRecordKey.getLabel();
            edgeDef = schema.getSchemaElement(label);
            SchemaElement srcVertexDef = schema.getSchemaElement(srcVertexRecordKey.getLabel());
            SchemaElement dstVertexDef = schema.getSchemaElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals = parseRawProperties(srcVertexDef,
                    srcVertexRecordKey.getProperties());
            long srcVertexHashId = getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals = parseRawProperties(dstVertexDef,
                    dstVertexRecordKey.getProperties());
            long dstVertexHashId = getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = edgeRecordKey.getEdgeInnerId();
            edgeId = new EdgeId(new VertexId(srcVertexHashId), new VertexId(dstVertexHashId), edgeInnerId);
            edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(new LabelId(labelId))
                    .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                    .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                    .build();
        }
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(edgeDef, properties);
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, true));
        batchBuilder.addOperation(new UpdateEdgeOperation(edgeId, edgeKind, propertyVals, false));
    }

    private void addOverwriteEdgeOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                           DataRecord dataRecord) {
        EdgeId edgeId;
        EdgeKind edgeKind;
        SchemaElement edgeDef;
        EdgeTarget edgeTarget = dataRecord.getEdgeTarget();
        Map<String, Object> properties = dataRecord.getProperties();
        if (edgeTarget != null) {
            edgeId = edgeTarget.getEdgeId();
            edgeKind = edgeTarget.getEdgeKind();
            edgeDef = schema.getSchemaElement(edgeKind.getEdgeLabelId().getId());
        } else {
            EdgeRecordKey edgeRecordKey = dataRecord.getEdgeRecordKey();
            VertexRecordKey srcVertexRecordKey = edgeRecordKey.getSrcVertexRecordKey();
            VertexRecordKey dstVertexRecordKey = edgeRecordKey.getDstVertexRecordKey();
            String label = edgeRecordKey.getLabel();
            edgeDef = schema.getSchemaElement(label);
            SchemaElement srcVertexDef = schema.getSchemaElement(srcVertexRecordKey.getLabel());
            SchemaElement dstVertexDef = schema.getSchemaElement(dstVertexRecordKey.getLabel());
            int labelId = edgeDef.getLabelId();
            Map<Integer, PropertyValue> srcVertexPkVals = parseRawProperties(srcVertexDef,
                    srcVertexRecordKey.getProperties());
            long srcVertexHashId = getHashId(srcVertexDef.getLabelId(), srcVertexPkVals, srcVertexDef);
            Map<Integer, PropertyValue> dstVertexPkVals = parseRawProperties(dstVertexDef,
                    dstVertexRecordKey.getProperties());
            long dstVertexHashId = getHashId(dstVertexDef.getLabelId(), dstVertexPkVals, dstVertexDef);
            long edgeInnerId = this.edgeIdGenerator.getNextId();
            edgeId = new EdgeId(new VertexId(srcVertexHashId), new VertexId(dstVertexHashId), edgeInnerId);
            edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(new LabelId(labelId))
                    .setSrcVertexLabelId(new LabelId(srcVertexDef.getLabelId()))
                    .setDstVertexLabelId(new LabelId(dstVertexDef.getLabelId()))
                    .build();
        }
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(edgeDef, properties);
        batchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, propertyVals, true));
        batchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, propertyVals, false));
    }

    private void addDeleteVertexOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                          DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        String label = vertexRecordKey.getLabel();
        SchemaElement vertexDef = schema.getSchemaElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals = parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        long hashId = getHashId(labelId, pkVals, vertexDef);
        batchBuilder.addOperation(new DeleteVertexOperation(new VertexId(hashId), new LabelId(labelId)));
    }

    private void addUpdateVertexOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                               DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        Map<String, Object> properties = dataRecord.getProperties();
        String label = vertexRecordKey.getLabel();
        SchemaElement vertexDef = schema.getSchemaElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals = parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        long hashId = getHashId(labelId, pkVals, vertexDef);
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        batchBuilder.addOperation(new UpdateVertexOperation(new VertexId(hashId), new LabelId(labelId), propertyVals));
    }

    private void addOverwriteVertexOperation(OperationBatch.Builder batchBuilder, GraphSchema schema,
                                                  DataRecord dataRecord) {
        VertexRecordKey vertexRecordKey = dataRecord.getVertexRecordKey();
        Map<String, Object> properties = dataRecord.getProperties();
        String label = vertexRecordKey.getLabel();
        SchemaElement vertexDef = schema.getSchemaElement(label);
        int labelId = vertexDef.getLabelId();
        Map<Integer, PropertyValue> pkVals = parseRawProperties(vertexDef, vertexRecordKey.getProperties());
        Map<Integer, PropertyValue> propertyVals = parseRawProperties(vertexDef, properties);
        propertyVals.putAll(pkVals);
        long hashId = getHashId(labelId, propertyVals, vertexDef);
        batchBuilder.addOperation(
                new OverwriteVertexOperation(new VertexId(hashId), new LabelId(labelId), propertyVals));
    }

    public static Map<Integer, PropertyValue> parseRawProperties(SchemaElement schemaElement,
                                                                 Map<String, Object> properties) {
        Map<Integer, PropertyValue> res = new HashMap<>();
        if (properties != null) {
            properties.forEach((propertyName, valString) -> {
                GraphProperty propertyDef = schemaElement.getProperty(propertyName);
                if (propertyDef == null) {
                    throw new PropertyDefNotFoundException("property [" + propertyName + "] not found in [" +
                            schemaElement.getLabel() + "]");
                }
                int id = propertyDef.getId();
                DataType dataType = propertyDef.getDataType();
                PropertyValue propertyValue = new PropertyValue(dataType, valString);
                res.put(id, propertyValue);
            });
        }
        return res;
    }

    public static long getHashId(int labelId, Map<Integer, PropertyValue> pkVals, SchemaElement schemaElement) {
        List<Integer> pkIdxs = schemaElement.getPkPropertyIndices();
        List<GraphProperty> propertyDefs = schemaElement.getPropertyList();
        List<byte[]> pks = new ArrayList<>(pkIdxs.size());
        for (int pkIdx : pkIdxs) {
            int propertyId = propertyDefs.get(pkIdx).getId();
            byte[] valBytes = pkVals.get(propertyId).getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
    }

}
