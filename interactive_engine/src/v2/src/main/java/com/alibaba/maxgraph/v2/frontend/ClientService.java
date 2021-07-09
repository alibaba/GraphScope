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

import com.alibaba.maxgraph.proto.v2.*;
import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.exception.PropertyDefNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.MaxGraphWriter;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.*;
import com.alibaba.maxgraph.v2.common.metrics.MetricsAggregator;
import com.alibaba.maxgraph.v2.common.operation.EdgeId;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.VertexId;
import com.alibaba.maxgraph.v2.common.operation.dml.OverwriteEdgeOperation;
import com.alibaba.maxgraph.v2.common.operation.dml.OverwriteVertexOperation;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.schema.*;
import com.alibaba.maxgraph.v2.common.util.PkHashUtils;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import com.alibaba.maxgraph.v2.frontend.compiler.client.QueryStoreRpcClient;
import com.alibaba.maxgraph.v2.sdk.DataLoadTarget;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientService extends ClientGrpc.ClientImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ClientService.class);

    private static final long COMMIT_TIMEOUT_SEC = 30;

    private RealtimeWriter realtimeWriter;
    private SnapshotCache snapshotCache;
    private MetricsAggregator metricsAggregator;
    private StoreIngestor storeIngestor;
    private MetaService metaService;
    private MaxGraphWriter writer;

    public ClientService(RealtimeWriter realtimeWriter, SnapshotCache snapshotCache,
                         MetricsAggregator metricsAggregator, StoreIngestor storeIngestor, MetaService metaService,
                         RoleClients<QueryStoreRpcClient> queryStoreClients, MaxGraphWriter writer) {
        this.realtimeWriter = realtimeWriter;
        this.snapshotCache = snapshotCache;
        this.metricsAggregator = metricsAggregator;
        this.storeIngestor = storeIngestor;
        this.metaService = metaService;
        this.writer = writer;
    }

    @Override
    public void getPartitionNum(GetPartitionNumRequest request, StreamObserver<GetPartitionNumResponse> responseObserver) {
        int partitionCount = metaService.getPartitionCount();
        responseObserver.onNext(GetPartitionNumResponse.newBuilder().setPartitionNum(partitionCount).build());
        responseObserver.onCompleted();
    }

    @Override
    public void prepareDataLoad(PrepareDataLoadRequest request, StreamObserver<PrepareDataLoadResponse> responseObserver) {
        for (DataLoadTargetPb dataLoadTargetPb : request.getDataLoadTargetsList()) {
            DataLoadTarget dataLoadTarget = DataLoadTarget.parseProto(dataLoadTargetPb);
            ((MaxGraphWriterImpl)this.writer).prepareDataLoad(dataLoadTarget);
        }
        try {
            this.writer.commit().get(COMMIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("commit prepare data load failed", e);
            throw new RuntimeException(e);
        }
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        responseObserver.onNext(PrepareDataLoadResponse.newBuilder().setGraphDef(graphDef.toProto()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void commitDataLoad(CommitDataLoadRequest request, StreamObserver<CommitDataLoadResponse> responseObserver) {
        Map<Long, DataLoadTargetPb> tableToTarget = request.getTableToTargetMap();
        tableToTarget.forEach((tableId, targetPb) -> {
            DataLoadTarget dataLoadTarget = DataLoadTarget.parseProto(targetPb);
            ((MaxGraphWriterImpl)this.writer).commitDataLoad(dataLoadTarget, tableId);
        });
        try {
            this.writer.commit().get(COMMIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("commit prepare data load failed", e);
            throw new RuntimeException(e);
        }
        responseObserver.onNext(CommitDataLoadResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadJsonSchema(LoadJsonSchemaRequest request, StreamObserver<LoadJsonSchemaResponse> responseObserver) {
        String schemaJson = request.getSchemaJson();
        GraphSchema graphSchema = GraphSchemaMapper.parseFromJson(schemaJson).toGraphSchema();
        this.writer.setAutoCommit(false);
        for (VertexType vertexType : graphSchema.getVertexTypes()) {
            this.writer.createVertexType(vertexType.getLabel(), vertexType.getPropertyList(), vertexType.getPrimaryKeyConstraint().getPrimaryKeyList());
        }
        for (EdgeType edgeType : graphSchema.getEdgeTypes()) {
            this.writer.createEdgeType(edgeType.getLabel(), edgeType.getPropertyList(), edgeType.getRelationList());
        }
        try {
            this.writer.commit().get(COMMIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("commit failed", e);
            throw new RuntimeException(e);
        }
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        responseObserver.onNext(LoadJsonSchemaResponse.newBuilder().setGraphDef(graphDef.toProto()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void dropSchema(DropSchemaRequest request, StreamObserver<DropSchemaResponse> responseObserver) {
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        for (EdgeType edgeType : graphDef.getEdgeTypes()) {
            String label = edgeType.getLabel();
            for (EdgeRelation relation : edgeType.getRelationList()) {
                this.writer.dropEdgeRelation(label, relation.getSource().getLabel(), relation.getTarget().getLabel());
            }
            this.writer.dropEdgeType(label);
        }
        for (VertexType vertexType : graphDef.getVertexTypes()) {
            this.writer.dropVertexType(vertexType.getLabel());
        }
        try {
            this.writer.commit().get(COMMIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("drop schema commit failed", e);
            throw new RuntimeException(e);
        }
        responseObserver.onNext(DropSchemaResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMetrics(GetMetricsRequest request, StreamObserver<GetMetricsResponse> responseObserver) {
        String roleNames = request.getRoleNames();
        this.metricsAggregator.aggregateMetricsJson(roleNames, new CompletionCallback<String>() {
            @Override
            public void onCompleted(String res) {
                responseObserver.onNext(GetMetricsResponse.newBuilder().setMetricsJson(res).build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                logger.error("get metrics failed", t);
                responseObserver.onError(t);
            }
        });

    }

    @Override
    public void addVertices(AddVerticesRequest request, StreamObserver<AddVerticesResponse> responseObserver) {
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        String session = request.getSession();
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
        for (VertexDataPb vertexDataPb : request.getDataListList()) {
            String label = vertexDataPb.getLabel();
            Map<String, String> properties = vertexDataPb.getPropertiesMap();
            TypeDef typeDef = graphDef.getTypeDef(label);
            LabelId labelId = typeDef.getTypeLabelId();
            Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);
            List<Integer> pkIdxs = typeDef.getPkIdxs();
            List<PropertyDef> propertyDefs = typeDef.getProperties();
            long hashId = getHashId(labelId.getId(), operationProperties, pkIdxs, propertyDefs);
            batchBuilder.addOperation(new OverwriteVertexOperation(new VertexId(hashId), labelId, operationProperties));
        }
        BatchId batchId = this.realtimeWriter.writeOperations(UuidUtils.getBase64UUIDString(), session,
                batchBuilder.build());
        responseObserver.onNext(AddVerticesResponse.newBuilder().setSnapshotId(batchId.getSnapshotId()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void addEdges(AddEdgesRequest request, StreamObserver<AddEdgesResponse> responseObserver) {
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        String session = request.getSession();
        long innerId = System.nanoTime();
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
        for (EdgeDataPb edgeDataPb : request.getDataListList()) {
            String label = edgeDataPb.getLabel();
            String srcLabel = edgeDataPb.getSrcLabel();
            String dstLabel = edgeDataPb.getDstLabel();
            Map<String, String> srcPkMap = edgeDataPb.getSrcPkMap();
            Map<String, String> dstPkMap = edgeDataPb.getDstPkMap();
            Map<String, String> properties = edgeDataPb.getPropertiesMap();

            TypeDef typeDef = graphDef.getTypeDef(label);
            LabelId labelId = typeDef.getTypeLabelId();
            TypeDef srcTypeDef = graphDef.getTypeDef(srcLabel);
            LabelId srcLabelId = srcTypeDef.getTypeLabelId();
            TypeDef dstTypeDef = graphDef.getTypeDef(dstLabel);
            LabelId dstLabelId = dstTypeDef.getTypeLabelId();
            long srcHashId = getHashId(srcLabelId.getId(), buildPropertiesMap(srcTypeDef, srcPkMap),
                    srcTypeDef.getPkIdxs(), srcTypeDef.getProperties());
            long dstHashId = getHashId(dstLabelId.getId(), buildPropertiesMap(dstTypeDef, dstPkMap),
                    dstTypeDef.getPkIdxs(), dstTypeDef.getProperties());
            EdgeId edgeId = new EdgeId(new VertexId(srcHashId), new VertexId(dstHashId), innerId);
            innerId++;
            EdgeKind edgeKind = EdgeKind.newBuilder()
                    .setEdgeLabelId(labelId)
                    .setSrcVertexLabelId(srcLabelId)
                    .setDstVertexLabelId(dstLabelId)
                    .build();
            Map<Integer, PropertyValue> operationProperties = buildPropertiesMap(typeDef, properties);
            batchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, operationProperties, true));
            batchBuilder.addOperation(new OverwriteEdgeOperation(edgeId, edgeKind, operationProperties, false));
        }
        BatchId batchId = this.realtimeWriter.writeOperations(UuidUtils.getBase64UUIDString(), session,
                batchBuilder.build());
        responseObserver.onNext(AddEdgesResponse.newBuilder().setSnapshotId(batchId.getSnapshotId()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void remoteFlush(RemoteFlushRequest request, StreamObserver<RemoteFlushResponse> responseObserver) {
        long snapshotId = request.getSnapshotId();
        this.realtimeWriter.waitForSnapshotCompletion(snapshotId);
        responseObserver.onNext(RemoteFlushResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private long getHashId(int labelId, Map<Integer, PropertyValue> operationProperties, List<Integer> pkIdxs,
                           List<PropertyDef> propertyDefs) {
        List<byte[]> pks = new ArrayList<>(pkIdxs.size());
        for (int pkIdx : pkIdxs) {
            int propertyId = propertyDefs.get(pkIdx).getId();
            byte[] valBytes = operationProperties.get(propertyId).getValBytes();
            pks.add(valBytes);
        }
        return PkHashUtils.hash(labelId, pks);
    }

    private Map<Integer, PropertyValue> buildPropertiesMap(TypeDef typeDef, Map<String, String> properties) {
        Map<Integer, PropertyValue> operationProperties = new HashMap<>(properties.size());
        properties.forEach((propertyName, valString) -> {
            GraphProperty propertyDef = typeDef.getProperty(propertyName);
            if (propertyDef == null) {
                throw new PropertyDefNotFoundException("property [" + propertyName + "] not found in [" +
                        typeDef.getLabel() + "]");
            }
            int id = propertyDef.getId();
            DataType dataType = propertyDef.getDataType();
            PropertyValue propertyValue = new PropertyValue(dataType, valString);
            operationProperties.put(id, propertyValue);
        });
        return operationProperties;
    }

    @Override
    public void getSchema(GetSchemaRequest request, StreamObserver<GetSchemaResponse> observer) {
        GraphDef graphDef = this.snapshotCache.getSnapshotWithSchema().getGraphDef();
        observer.onNext(GetSchemaResponse.newBuilder()
                .setGraphDef(graphDef.toProto()).build());
        observer.onCompleted();
    }

    @Override
    public void ingestData(IngestDataRequest request, StreamObserver<IngestDataResponse> responseObserver) {
        String dataPath = request.getDataPath();
        logger.info("ingestData. path [" + dataPath + "]");
        int storeCount = this.metaService.getStoreCount();
        AtomicInteger counter = new AtomicInteger(storeCount);
        AtomicBoolean finished = new AtomicBoolean(false);
        for (int i = 0; i < storeCount; i++) {
            this.storeIngestor.ingest(i, dataPath, new CompletionCallback<Void>() {
                @Override
                public void onCompleted(Void res) {
                    if (!finished.get() && counter.decrementAndGet() == 0) {
                        finish(null);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("failed ingest", t);
                    finish(t);
                }

                private void finish(Throwable t) {
                    if (finished.getAndSet(true)) {
                        return;
                    }
                    logger.info("ingest finished. Error [" + t + "]");
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onNext(IngestDataResponse.newBuilder().build());
                        responseObserver.onCompleted();
                    }
                }
            });
        }
    }
}
