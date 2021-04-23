package com.alibaba.maxgraph.v2.common.frontend.remote;

import com.alibaba.maxgraph.proto.v2.LabelVertexEdgeId;
import com.alibaba.maxgraph.proto.v2.StoreVertexIdsResponse;
import com.alibaba.maxgraph.proto.v2.StoreVertexIdsResult;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.graph.structure.MaxGraphVertex;
import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class StoreVertexIdsResponseObserver implements StreamObserver<StoreVertexIdsResponse> {
    private static final Logger logger = LoggerFactory.getLogger(StoreVerticesResponseObserver.class);

    private CountDownLatch latch;
    private List<Vertex> vertexList;
    private GraphSchema schema;
    private SnapshotMaxGraph graph;

    public StoreVertexIdsResponseObserver(CountDownLatch latch,
                                         GraphSchema schema,
                                         SnapshotMaxGraph graph) {
        this.latch = latch;
        this.vertexList = Lists.newCopyOnWriteArrayList();
        this.schema = schema;
        this.graph = graph;
    }

    @Override
    public void onNext(StoreVertexIdsResponse storeVertexIdsResponse) {
        if (0 != storeVertexIdsResponse.getErrorCode()) {
            throw new RuntimeException("query vertex ids faile for " + storeVertexIdsResponse.getErrorMessage());
        }
        for (StoreVertexIdsResult storeVertexIdsResult : storeVertexIdsResponse.getResultsList()) {
            for (LabelVertexEdgeId labelVertexEdgeId : storeVertexIdsResult.getTargetVertexIdsList()) {
                String label = this.schema.getSchemaElement(labelVertexEdgeId.getLabelId()).getLabel();
                vertexList.add(new MaxGraphVertex(this.graph,
                        new CompositeId(labelVertexEdgeId.getId(), labelVertexEdgeId.getLabelId()),
                        label));
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("query store vertex ids failed", throwable);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    List<Vertex> getVertexList() {
        return this.vertexList;
    }
}
