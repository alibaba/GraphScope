/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.client.write;

import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.ExecutionException;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptTable;
import com.alibaba.graphscope.common.ir.rel.ddl.GraphTableModify;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.common.Status;
import com.alibaba.graphscope.interactive.models.EdgeRequest;
import com.alibaba.graphscope.interactive.models.Property;
import com.alibaba.graphscope.interactive.models.VertexEdgeRequest;
import com.alibaba.graphscope.interactive.models.VertexRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class HttpWriteClient {
    private final Session session;

    public HttpWriteClient(Session session) {
        this.session = session;
    }

    public void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            IrMeta irMeta,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger) {
        LogicalPlan plan = request.getRequestLogical();
        Preconditions.checkArgument(plan.getMode() == LogicalPlan.Mode.WRITE_ONLY);
        String graphId = (String) irMeta.getGraphId().getId();
        RequestBuilder builder = new RequestBuilder();
        buildRequest(plan.getRegularQuery(), builder);
        List<Request> requests = builder.build();
        List<Result> results = Lists.newArrayList();
        requests.forEach(
                req -> {
                    switch (req.type) {
                        case ADD_VERTEX:
                            results.add(
                                    session.addVertex(graphId, (VertexEdgeRequest) req.request));
                            break;
                        case ADD_EDGE:
                            results.add(session.addEdge(graphId, (BatchEdgeRequest) req.request));
                            break;
                    }
                });
        for (Result result : results) {
            if (!result.isOk()) {
                listener.onError(
                        new ExecutionException(
                                "write error from execution. " + toString(result.getStatus()),
                                null));
            }
        }
        listener.onCompleted();
    }

    private String toString(Status status) {
        return status.getCode() + ":" + status.getMessage();
    }

    private void buildRequest(RelNode top, RequestBuilder builder) {
        top.getInputs().forEach(input -> buildRequest(input, builder));
        if (top instanceof GraphTableModify) {
            builder.addWriteOp((GraphTableModify) top);
        }
    }

    public void close() throws Exception {
        if (this.session != null) {
            this.session.close();
        }
    }

    private enum Type {
        ADD_VERTEX,
        ADD_EDGE,
    }

    private final class Request<T> {
        public final Type type;
        public final T request;

        public Request(Type type, T request) {
            this.type = type;
            this.request = request;
        }
    }

    private final class BatchEdgeRequest implements List<EdgeRequest> {
        private final List<EdgeRequest> inner;

        public BatchEdgeRequest() {
            this.inner = new ArrayList<>();
        }

        @Override
        public int size() {
            return this.inner.size();
        }

        @Override
        public boolean isEmpty() {
            return this.inner.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return this.inner.contains(o);
        }

        @NotNull
        @Override
        public Iterator<EdgeRequest> iterator() {
            return this.inner.iterator();
        }

        @NotNull
        @Override
        public Object[] toArray() {
            return this.inner.toArray();
        }

        @NotNull
        @Override
        public <T> T[] toArray(@NotNull T[] a) {
            return this.inner.toArray(a);
        }

        @Override
        public boolean add(EdgeRequest edgeRequest) {
            return this.inner.add(edgeRequest);
        }

        @Override
        public boolean remove(Object o) {
            return this.inner.remove(o);
        }

        @Override
        public boolean containsAll(@NotNull Collection<?> c) {
            return this.inner.containsAll(c);
        }

        @Override
        public boolean addAll(@NotNull Collection<? extends EdgeRequest> c) {
            return this.inner.addAll(c);
        }

        @Override
        public boolean addAll(int index, @NotNull Collection<? extends EdgeRequest> c) {
            return this.inner.addAll(index, c);
        }

        @Override
        public boolean removeAll(@NotNull Collection<?> c) {
            return this.inner.removeAll(c);
        }

        @Override
        public boolean retainAll(@NotNull Collection<?> c) {
            return this.inner.retainAll(c);
        }

        @Override
        public void clear() {
            this.inner.clear();
        }

        @Override
        public EdgeRequest get(int index) {
            return this.inner.get(index);
        }

        @Override
        public EdgeRequest set(int index, EdgeRequest element) {
            return this.inner.set(index, element);
        }

        @Override
        public void add(int index, EdgeRequest element) {
            this.inner.add(index, element);
        }

        @Override
        public EdgeRequest remove(int index) {
            return this.inner.remove(index);
        }

        @Override
        public int indexOf(Object o) {
            return this.inner.indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return this.inner.lastIndexOf(o);
        }

        @NotNull
        @Override
        public ListIterator<EdgeRequest> listIterator() {
            return this.inner.listIterator();
        }

        @NotNull
        @Override
        public ListIterator<EdgeRequest> listIterator(int index) {
            return this.inner.listIterator(index);
        }

        @NotNull
        @Override
        public List<EdgeRequest> subList(int fromIndex, int toIndex) {
            return this.inner.subList(fromIndex, toIndex);
        }
    }

    private final class RequestBuilder {
        private final List<Request> requests;

        public RequestBuilder() {
            this.requests = Lists.newArrayList();
        }

        public List<Request> build() {
            return Collections.unmodifiableList(this.requests);
        }

        public RequestBuilder addWriteOp(GraphTableModify writeOp) {
            Type type = analyzeType(writeOp);
            Request lastRequest = null;
            if (!requests.isEmpty()) {
                lastRequest = requests.get(requests.size() - 1);
            }
            if (type == Type.ADD_VERTEX) {
                VertexRequest request =
                        createVertex(((GraphTableModify.Insert) writeOp).getTargetGraph());
                if (lastRequest != null
                        && lastRequest.type == Type.ADD_VERTEX
                        && lastRequest.request instanceof VertexEdgeRequest) {
                    ((VertexEdgeRequest) lastRequest.request).addVertexRequestItem(request);
                } else {
                    VertexEdgeRequest vertexReq = new VertexEdgeRequest();
                    vertexReq.addVertexRequestItem(request);
                    requests.add(new Request(Type.ADD_VERTEX, vertexReq));
                }
            } else if (type == Type.ADD_EDGE) {
                EdgeRequest request =
                        createEdge(
                                (TargetGraph.Edge)
                                        ((GraphTableModify.Insert) writeOp).getTargetGraph());
                if (lastRequest != null
                        && lastRequest.type == Type.ADD_VERTEX
                        && lastRequest.request instanceof VertexEdgeRequest) {
                    ((VertexEdgeRequest) lastRequest.request).addEdgeRequestItem(request);
                } else if (lastRequest != null
                        && lastRequest.type == Type.ADD_EDGE
                        && lastRequest.request instanceof BatchEdgeRequest) {
                    ((BatchEdgeRequest) lastRequest.request).add(request);
                } else {
                    BatchEdgeRequest batchRequest = new BatchEdgeRequest();
                    batchRequest.add(request);
                    requests.add(new Request(Type.ADD_EDGE, batchRequest));
                }
            }
            return this;
        }

        private VertexRequest createVertex(TargetGraph vertex) {
            GraphOptTable optTable = (GraphOptTable) vertex.getOptTable();
            GraphSchemaType schemaType = vertex.getSingleSchemaType();
            Preconditions.checkArgument(schemaType.getScanOpt() == GraphOpt.Source.VERTEX);
            GraphLabelType labelType = schemaType.getLabelType();
            Preconditions.checkArgument(labelType.getLabelsEntry().size() == 1);
            VertexRequest request = new VertexRequest();
            request.setLabel(labelType.getLabelsEntry().get(0).getLabel());
            FieldMappings mappings = vertex.getMappings();
            mappings.getMappings()
                    .forEach(
                            e -> {
                                RexNode source = e.getSource();
                                Preconditions.checkArgument(source instanceof RexLiteral);
                                Object value =
                                        Utils.getValuesAsList(((RexLiteral) source).getValue())
                                                .get(0);
                                RexNode target = e.getTarget();
                                Preconditions.checkArgument(
                                        target instanceof RexGraphVariable
                                                && ((RexGraphVariable) target).getProperty()
                                                        != null);
                                GraphProperty property = ((RexGraphVariable) target).getProperty();
                                ImmutableBitSet propertyIds =
                                        Utils.getPropertyIds(property, schemaType);
                                if (optTable.isKey(propertyIds)) {
                                    request.setPrimaryKeyValue(value);
                                } else {
                                    String propertyName =
                                            ((RexGraphVariable) target).getName().split("\\.")[1];
                                    Property prop = new Property();
                                    prop.setName(propertyName);
                                    prop.setValue(value);
                                    request.addPropertiesItem(prop);
                                }
                            });
            return request;
        }

        private EdgeRequest createEdge(TargetGraph.Edge edge) {
            GraphSchemaType schemaType = edge.getSingleSchemaType();
            Preconditions.checkArgument(schemaType.getScanOpt() == GraphOpt.Source.EDGE);
            GraphLabelType labelType = schemaType.getLabelType();
            Preconditions.checkArgument(labelType.getLabelsEntry().size() == 1);
            EdgeRequest request = new EdgeRequest();
            request.setEdgeLabel(labelType.getLabelsEntry().get(0).getLabel());
            request.setSrcLabel(labelType.getLabelsEntry().get(0).getSrcLabel());
            request.setDstLabel(labelType.getLabelsEntry().get(0).getDstLabel());

            VertexRequest srcVertex = createVertex(edge.getSrcVertex());
            VertexRequest dstVertex = createVertex(edge.getDstVertex());

            if (srcVertex.getPrimaryKeyValue() != null) {
                request.setSrcPrimaryKeyValue(srcVertex.getPrimaryKeyValue());
            }

            if (dstVertex.getPrimaryKeyValue() != null) {
                request.setDstPrimaryKeyValue(dstVertex.getPrimaryKeyValue());
            }

            FieldMappings mappings = edge.getMappings();
            mappings.getMappings()
                    .forEach(
                            e -> {
                                RexNode source = e.getSource();
                                Preconditions.checkArgument(source instanceof RexLiteral);
                                Object value =
                                        Utils.getValuesAsList(((RexLiteral) source).getValue())
                                                .get(0);
                                RexNode target = e.getTarget();
                                Preconditions.checkArgument(
                                        target instanceof RexGraphVariable
                                                && ((RexGraphVariable) target).getProperty()
                                                        != null);
                                String propertyName =
                                        ((RexGraphVariable) target).getName().split("\\.")[1];
                                Property prop = new Property();
                                prop.setName(propertyName);
                                prop.setValue(value);
                                request.addPropertiesItem(prop);
                            });
            return request;
        }

        private Type analyzeType(GraphTableModify writeOp) {
            if (writeOp instanceof GraphTableModify.Insert) {
                TargetGraph targetGraph = ((GraphTableModify.Insert) writeOp).getTargetGraph();
                if (targetGraph.getSingleSchemaType().getScanOpt() == GraphOpt.Source.VERTEX) {
                    return Type.ADD_VERTEX;
                } else if (targetGraph.getSingleSchemaType().getScanOpt() == GraphOpt.Source.EDGE) {
                    return Type.ADD_EDGE;
                }
            }
            throw new IllegalArgumentException("unsupported write operation: " + writeOp);
        }
    }
}
