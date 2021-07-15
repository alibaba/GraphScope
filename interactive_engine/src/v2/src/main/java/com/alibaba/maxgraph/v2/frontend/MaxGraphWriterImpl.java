/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphWriteDataException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.structure.ElementId;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.cache.MaxGraphCache;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.VertexId;
import com.alibaba.maxgraph.v2.common.operation.dml.DeleteEdgeOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.DeleteVertexOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.OverwriteEdgeOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.OverwriteVertexOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.UpdateEdgeOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.UpdateVertexOperation;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.EdgeKind;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.common.schema.PropertyValue;
import com.alibaba.maxgraph.v2.common.schema.TypeDef;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.common.schema.ddl.DdlExecutors;
import com.alibaba.maxgraph.v2.common.schema.request.*;
import com.alibaba.maxgraph.v2.common.util.PkHashUtils;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Remote max graph writer class, each query will create one graph reader and writer,
 * each reader and writer will belong to only one query, so will can cache the reader/writer
 * result in the instance, and the cache will be release memory when the query is finished
 */
public class MaxGraphWriterImpl implements MaxGraphWriter {

    private static final Logger logger = LoggerFactory.getLogger(MaxGraphWriterImpl.class);

    private RealtimeWriter realtimeWriter;
    private OperationBatch.Builder operationBatchBuilder;

    private SchemaWriter schemaWriter;
    private DdlRequestBatch.Builder ddlBatchBuilder;

    private List<CompletionCallback<Object>> pendingCallbacks;

    private DdlExecutors ddlExecutors;
    private SnapshotCache snapshotCache;
    private String sessionId;
    private boolean autoCommit;

    enum BatchType {
        /**
         * DDL batch
         */
        Ddl,

        /**
         * realtime write batch
         */
        REALTIME,

        /**
         * unknown batch
         */
        UNKNOWN;
    }

    private BatchType batchType;

    private MaxGraphCache cache;

    public MaxGraphWriterImpl(RealtimeWriter realtimeWriter, SchemaWriter schemaWriter, DdlExecutors ddlExecutors,
                              SnapshotCache snapshotCache, String sessionId, boolean autoCommit, MaxGraphCache cache) {
        this.realtimeWriter = realtimeWriter;
        this.schemaWriter = schemaWriter;
        this.ddlExecutors = ddlExecutors;
        this.snapshotCache = snapshotCache;
        this.sessionId = sessionId;
        this.autoCommit = autoCommit;

        this.batchType = BatchType.UNKNOWN;
        this.operationBatchBuilder = OperationBatch.newBuilder();
        this.ddlBatchBuilder = DdlRequestBatch.newBuilder();
        this.pendingCallbacks = new ArrayList<>();
        this.cache = cache;
    }

    private void ensureState(BatchType batchType) {
        if (this.snapshotCache.getSnapshotWithSchema().getSnapshotId() < 0L) {
            throw new IllegalStateException("Waiting for coordinator snapshot information");
        }
        if (this.batchType == BatchType.UNKNOWN) {
            this.batchType = batchType;
            return;
        }
        if (this.batchType != batchType) {
            throw new IllegalStateException("Current batch is [" + this.batchType + "], cannot add [" + batchType +
                    "] request");
        }
    }

    private void maybeCommit() {
        if (this.autoCommit) {
            this.commit();
        }
    }

    public Future<Void> prepareDataLoad(DataLoadTarget dataLoadTarget) {
        this.ensureState(BatchType.Ddl);
        PrepareDataLoadRequest ddlRequest = new PrepareDataLoadRequest(dataLoadTarget);
        this.ddlBatchBuilder.addDdlRequest(ddlRequest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("prepareDataLoad callback error. " +
                        "DataLoadTarget [" + dataLoadTarget + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    public Future<Void> commitDataLoad(DataLoadTarget dataLoadTarget, long tableId) {
        this.ensureState(BatchType.Ddl);
        CommitDataLoadRequest ddlRequest = new CommitDataLoadRequest(dataLoadTarget, tableId);
        this.ddlBatchBuilder.addDdlRequest(ddlRequest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("prepareDataLoad callback error. " +
                        "DataLoadTarget [" + dataLoadTarget + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Integer> createVertexType(String label, List<GraphProperty> propertyList, List<String> primaryKeyList)
            throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        TypeDef.Builder typeDefBuilder = TypeDef.newBuilder();
        typeDefBuilder.setLabel(label);
        typeDefBuilder.setTypeEnum(TypeEnum.VERTEX);

        Set<String> primaryKeys = new HashSet<>(primaryKeyList);
        for (GraphProperty graphProperty : propertyList) {
            PropertyDef.Builder propertyBuilder = PropertyDef.newBuilder();
            String propertyName = graphProperty.getName();
            DataType dataType = graphProperty.getDataType();
            propertyBuilder.setName(propertyName)
                    .setDataType(dataType)
                    .setComment(graphProperty.getComment());
            if (primaryKeys.contains(propertyName)) {
                propertyBuilder.setPk(true);
            }
            if (graphProperty.hasDefaultValue()) {
                Object defaultValue = graphProperty.getDefaultValue();
                propertyBuilder.setDefaultValue(new PropertyValue(dataType, defaultValue));
            }
            typeDefBuilder.addPropertyDef(propertyBuilder.build());
        }
        CreateVertexTypeRequest ddlRequest = new CreateVertexTypeRequest(typeDefBuilder.build());
        this.ddlBatchBuilder.addDdlRequest(ddlRequest);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                GraphDef graphDef = (GraphDef) res;
                future.complete(graphDef.getLabelId(label).getId());
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("createVertexType callback error. label [" +
                        label + "]", t));
            }
        });

        maybeCommit();
        return future;
    }

    @Override
    public Future<Integer> createEdgeType(String label, List<GraphProperty> propertyList,
                                          List<EdgeRelation> relationList) throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        TypeDef.Builder typeDefBuilder = TypeDef.newBuilder();
        typeDefBuilder.setLabel(label);
        typeDefBuilder.setTypeEnum(TypeEnum.EDGE);

        for (GraphProperty graphProperty : propertyList) {
            PropertyDef.Builder propertyBuilder = PropertyDef.newBuilder();
            String propertyName = graphProperty.getName();
            DataType dataType = graphProperty.getDataType();
            propertyBuilder.setName(propertyName)
                    .setDataType(dataType)
                    .setComment(graphProperty.getComment());

            if (graphProperty.hasDefaultValue()) {
                Object defaultValue = graphProperty.getDefaultValue();
                propertyBuilder.setDefaultValue(new PropertyValue(dataType, defaultValue));
            }
            typeDefBuilder.addPropertyDef(propertyBuilder.build());
        }
        CreateEdgeTypeRequest createEdgeTypeRequest = new CreateEdgeTypeRequest(typeDefBuilder.build());
        this.ddlBatchBuilder.addDdlRequest(createEdgeTypeRequest);
        for (EdgeRelation edgeRelation : relationList) {
            AddEdgeKindRequest addEdgeKindRequest = new AddEdgeKindRequest(EdgeKind.newBuilder()
                    .setEdgeLabel(label)
                    .setSrcVertexLabel(edgeRelation.getSource().getLabel())
                    .setDstVertexLabel(edgeRelation.getTarget().getLabel())
                    .build());
            this.ddlBatchBuilder.addDdlRequest(addEdgeKindRequest);
        }

        CompletableFuture<Integer> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                GraphDef graphDef = (GraphDef) res;
                future.complete(graphDef.getLabelId(label).getId());
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("createEdgeType callback error. label [" +
                        label + "]", t));
            }
        });

        maybeCommit();
        return future;
    }

    @Override
    public Future<Integer> addProperty(String label, GraphProperty property) throws GraphCreateSchemaException {
        ///TODO this operation should update cache
        throw new UnsupportedOperationException("addProperty not supported");
    }

    @Override
    public Future<Void> dropProperty(String label, String property) throws GraphCreateSchemaException {
        ///TODO this operation should update cache
        throw new UnsupportedOperationException("dropProperty not supported");
    }

    @Override
    public Future<Void> addEdgeRelation(String edgeLabel, String sourceLabel, String destLabel)
            throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        EdgeKind edgeKind = EdgeKind.newBuilder().setEdgeLabel(edgeLabel)
                .setSrcVertexLabel(sourceLabel)
                .setDstVertexLabel(destLabel)
                .build();
        AddEdgeKindRequest addEdgeKindRequest = new AddEdgeKindRequest(edgeKind);
        this.ddlBatchBuilder.addDdlRequest(addEdgeKindRequest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("addEdgeRelation callback error. " +
                        "EdgeKind [" + edgeKind + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Void> dropEdgeRelation(String edgeLabel, String sourceLabel, String destLabel) throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        EdgeKind edgeKind = EdgeKind.newBuilder().setEdgeLabel(edgeLabel)
                .setSrcVertexLabel(sourceLabel)
                .setDstVertexLabel(destLabel)
                .build();
        RemoveEdgeKindRequest request = new RemoveEdgeKindRequest(edgeKind);
        this.ddlBatchBuilder.addDdlRequest(request);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("dropEdgeRelation callback error. " +
                        "EdgeKind [" + edgeKind + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Void> dropVertexType(String label) throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        DropVertexTypeRequest ddlRequest = new DropVertexTypeRequest(label);
        this.ddlBatchBuilder.addDdlRequest(ddlRequest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("dropVertexType callback error. label [" +
                        label + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Void> dropEdgeType(String label) throws GraphCreateSchemaException {
        this.ensureState(BatchType.Ddl);
        DropEdgeTypeRequest ddlRequest = new DropEdgeTypeRequest(label);
        this.ddlBatchBuilder.addDdlRequest(ddlRequest);
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphCreateSchemaException("dropEdgeType callback error. label [" +
                        label + "]", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public Future<Void> commit() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            List<CompletionCallback<Object>> callbacks = this.pendingCallbacks;
            switch (this.batchType) {
                case Ddl:
                    DdlRequestBatch ddlRequestBatch = ddlBatchBuilder.build();
                    try {
                        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
                        this.ddlExecutors.executeDdlRequestBatch(ddlRequestBatch, graphDef, 0);
                    } catch (Exception e) {
                        throw new GraphCreateSchemaException("try execute DDL batch failed", e);
                    }
                    long snapshotId = this.schemaWriter.submitBatchDdl(UuidUtils.getBase64UUIDString(),
                            this.sessionId, ddlRequestBatch);
                    this.snapshotCache.addListener(snapshotId, () -> {
                        for (CompletionCallback<Object> pendingCallback : callbacks) {
                            try {
                                pendingCallback.onCompleted(this.snapshotCache.getSnapshotWithSchema().getGraphDef());
                            } catch (Exception e) {
                                logger.error("ddl callback failed", e);
                            }
                        }
                        future.complete(null);
                    });
                    break;
                case REALTIME:
                    OperationBatch operationBatch = operationBatchBuilder.build();
                    BatchId batchId = this.realtimeWriter.writeOperations(UuidUtils.getBase64UUIDString(),
                            this.sessionId, operationBatch);
                    this.snapshotCache.addListener(batchId.getSnapshotId(), () -> {
                        for (CompletionCallback<Object> pendingCallback : callbacks) {
                            try {
                                pendingCallback.onCompleted(null);
                            } catch (Exception e) {
                                logger.error("realtime callback failed", e);
                            }
                        }
                        future.complete(null);
                    });
                    break;
                case UNKNOWN:
                    future.complete(null);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + this.batchType);
            }
        } catch (Exception e) {
            for (CompletionCallback<Object> pendingCallback : this.pendingCallbacks) {
                try {
                    pendingCallback.onError(e);
                } catch (Exception ex) {
                    logger.error("ddl callback failed", ex);
                }
            }
            future.completeExceptionally(e);
        }
        this.operationBatchBuilder = OperationBatch.newBuilder();
        this.ddlBatchBuilder = DdlRequestBatch.newBuilder();
        this.pendingCallbacks = new ArrayList<>();
        this.batchType = BatchType.UNKNOWN;
        return future;
    }

    @Override
    public Future<ElementId> insertVertex(String label, Map<String, Object> properties) throws GraphWriteDataException {
        Future<List<ElementId>> listFuture = insertVertices(Arrays.asList(Pair.of(label, properties)));
        return new Future<ElementId>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return listFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return listFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return listFuture.isDone();
            }

            @Override
            public ElementId get() throws InterruptedException, ExecutionException {
                List<ElementId> elementIds = listFuture.get();
                ElementId elementId = elementIds.get(0);
                cache.removeVertex(elementId);
                return elementId;
            }

            @Override
            public ElementId get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                List<ElementId> elementIds = listFuture.get(timeout, unit);
                ElementId elementId = elementIds.get(0);
                cache.removeVertex(elementId);
                return elementId;
            }
        };
    }

    @Override
    public Future<List<ElementId>> insertVertices(List<Pair<String, Map<String, Object>>> vertices) throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        List<ElementId> elementIds = new ArrayList<>(vertices.size());
        for (Pair<String, Map<String, Object>> vertex : vertices) {
            String label = vertex.getLeft();
            Map<String, Object> properties = vertex.getRight();
            TypeDef typeDef = graphDef.getTypeDef(label);
            LabelId labelId = typeDef.getTypeLabelId();
            Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);

            List<Integer> pkIdxs = typeDef.getPkIdxs();
            List<PropertyDef> propertyDefs = typeDef.getProperties();
            long hash64 = getHashId(labelId.getId(), operationProperties, pkIdxs, propertyDefs);
            VertexId vertexId = new VertexId(hash64);
            elementIds.add(new CompositeId(vertexId.getId(), labelId.getId()));

            OverwriteVertexOperation operation = new OverwriteVertexOperation(vertexId, labelId, operationProperties);
            this.operationBatchBuilder.addOperation(operation);
        }

        CompletableFuture<List<ElementId>> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                for (ElementId elementId : elementIds) {
                    cache.removeVertex(elementId);
                }
                future.complete(elementIds);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("insertVertices callback error", t));
            }
        });
        maybeCommit();
        return future;
    }

    public long getHashId(int labelId, Map<Integer, PropertyValue> operationProperties, List<Integer> pkIdxs,
                          List<PropertyDef> propertyDefs) {
        List<byte[]> pks = new ArrayList<>(pkIdxs.size());
        for (int pkIdx : pkIdxs) {
            int propertyId = propertyDefs.get(pkIdx).getId();
            PropertyValue pkValue = operationProperties.get(propertyId);
            if (pkValue == null) {
                throw new MaxGraphException("Cannot find pk value for property [" + propertyId + "]");
            }
            byte[] valBytes = pkValue.getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
    }

    private Map<Integer, PropertyValue> buildPropertiesMap(TypeDef typeDef, Map<String, Object> properties) {
        Map<Integer, PropertyValue> operationProperties = new HashMap<>(properties.size());
        properties.forEach((propertyName, valObject) -> {
            GraphProperty propertyDef = typeDef.getProperty(propertyName);
            int id = propertyDef.getId();
            DataType dataType = propertyDef.getDataType();
            PropertyValue propertyValue = new PropertyValue(dataType, valObject);
            operationProperties.put(id, propertyValue);
        });
        return operationProperties;
    }

    @Override
    public Future<Void> updateVertexProperties(ElementId vertexId, Map<String, Object> properties)
            throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        LabelId labelId = new LabelId(vertexId.labelId());
        TypeDef typeDef = graphDef.getTypeDef(labelId);
        Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);
        UpdateVertexOperation operation = new UpdateVertexOperation(new VertexId(vertexId.id()), labelId,
                operationProperties);
        this.operationBatchBuilder.addOperation(operation);

        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                cache.removeVertex(vertexId);
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("updateVertexProperties callback error", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Void> deleteVertex(ElementId vertexId) throws GraphWriteDataException {
        return deleteVertices(Collections.singleton(vertexId));
    }

    @Override
    public Future<Void> deleteVertices(Set<ElementId> elementIds) throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        for (ElementId elementId : elementIds) {
            DeleteVertexOperation operation = new DeleteVertexOperation(new VertexId(elementId.id()),
                    new LabelId(elementId.labelId()));
            this.operationBatchBuilder.addOperation(operation);
            this.cache.removeVertex(elementId);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("updateVertexProperties callback error", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<ElementId> insertEdge(ElementId srcId, ElementId destId, String label, Map<String, Object> properties)
            throws GraphWriteDataException {
        Future<List<ElementId>> listFuture = insertEdges(Arrays.asList(Triple.of(Pair.of(srcId, destId), label,
                properties)));
        return new Future<ElementId>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return listFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return listFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return listFuture.isDone();
            }

            @Override
            public ElementId get() throws InterruptedException, ExecutionException {
                List<ElementId> elementIds = listFuture.get();
                cache.removeOutEdgeList(srcId);
                cache.removeInEdgeList(destId);
                return elementIds.get(0);
            }

            @Override
            public ElementId get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                List<ElementId> elementIds = listFuture.get(timeout, unit);
                cache.removeOutEdgeList(srcId);
                cache.removeInEdgeList(destId);
                return elementIds.get(0);
            }
        };
    }

    @Override
    public Future<Void> updateEdgeProperties(ElementId srcId, ElementId destId, ElementId edgeId,
                                             Map<String, Object> properties) throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        EdgeId operationEdgeId = new EdgeId(new VertexId(srcId.id()), new VertexId(destId.id()), edgeId.id());
        LabelId edgeLabelId = new LabelId(edgeId.labelId());
        EdgeKind edgeKind = EdgeKind.newBuilder()
                .setEdgeLabelId(edgeLabelId)
                .setSrcVertexLabelId(new LabelId(srcId.labelId()))
                .setDstVertexLabelId(new LabelId(destId.labelId()))
                .build();

        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        TypeDef typeDef = graphDef.getTypeDef(edgeLabelId);
        Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);
        this.operationBatchBuilder.addOperation(new UpdateEdgeOperation(operationEdgeId, edgeKind, operationProperties,
                true));
        this.operationBatchBuilder.addOperation(new UpdateEdgeOperation(operationEdgeId, edgeKind, operationProperties,
                false));

        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                cache.removeOutEdgeList(srcId);
                cache.removeInEdgeList(destId);
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("updateEdgeProperties callback error", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<Void> deleteEdge(ElementId srcId, ElementId destId, ElementId edgeId) throws GraphWriteDataException {
        return deleteEdges(Arrays.asList(Triple.of(srcId, destId, edgeId)));
    }

    @Override
    public Future<Void> deleteEdges(List<Triple<ElementId, ElementId, ElementId>> edgeList) throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        for (Triple<ElementId, ElementId, ElementId> edge : edgeList) {
            ElementId srcElement = edge.getLeft();
            ElementId destElement = edge.getMiddle();
            ElementId edgeElement = edge.getRight();
            EdgeId edgeId = new EdgeId(new VertexId(srcElement.id()), new VertexId(destElement.id()), edgeElement.id());
            EdgeKind edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(new LabelId(edgeElement.labelId()))
                    .setSrcVertexLabelId(new LabelId(srcElement.labelId()))
                    .setDstVertexLabelId(new LabelId(destElement.labelId()))
                    .build();
            this.operationBatchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, true));
            this.operationBatchBuilder.addOperation(new DeleteEdgeOperation(edgeId, edgeKind, false));
            cache.removeOutEdgeList(srcElement);
            cache.removeInEdgeList(destElement);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(null);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("updateEdgeProperties callback error", t));
            }
        });
        maybeCommit();
        return future;
    }

    @Override
    public Future<List<ElementId>> insertEdges(List<Triple<Pair<ElementId, ElementId>, String,
            Map<String, Object>>> edges) throws GraphWriteDataException {
        this.ensureState(BatchType.REALTIME);

        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        List<ElementId> elementIds = new ArrayList<>(edges.size());
        long innerId = System.nanoTime();

        for (Triple<Pair<ElementId, ElementId>, String, Map<String, Object>> edge : edges) {
            Pair<ElementId, ElementId> left = edge.getLeft();
            ElementId srcElementId = left.getLeft();
            ElementId dstElementId = left.getRight();
            String label = edge.getMiddle();
            TypeDef typeDef = graphDef.getTypeDef(label);
            Map<String, Object> properties = edge.getRight();
            Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);

            LabelId labelId = typeDef.getTypeLabelId();
            List<Integer> pkIdxs = typeDef.getPkIdxs();
            long eid;
            if (pkIdxs != null && pkIdxs.size() > 0) {
                List<PropertyDef> propertyDefs = typeDef.getProperties();
                eid = getHashId(labelId.getId(), operationProperties, pkIdxs, propertyDefs);
            } else {
                eid = innerId++;
            }

            elementIds.add(new CompositeId(eid, typeDef.getLabelId()));
            EdgeId edgeId = new EdgeId(new VertexId(srcElementId.id()), new VertexId(dstElementId.id()), eid);
            EdgeKind edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(typeDef.getTypeLabelId())
                    .setSrcVertexLabelId(new LabelId(srcElementId.labelId()))
                    .setDstVertexLabelId(new LabelId(dstElementId.labelId()))
                    .build();
            this.operationBatchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, operationProperties, true));
            this.operationBatchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, operationProperties, false));
            cache.removeOutEdgeList(srcElementId);
            cache.removeInEdgeList(dstElementId);
        }

        CompletableFuture<List<ElementId>> future = new CompletableFuture<>();
        this.pendingCallbacks.add(new CompletionCallback<Object>() {
            @Override
            public void onCompleted(Object res) {
                future.complete(elementIds);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(new GraphWriteDataException("insertVertices callback error", t));
            }
        });
        maybeCommit();
        return future;
    }
}
