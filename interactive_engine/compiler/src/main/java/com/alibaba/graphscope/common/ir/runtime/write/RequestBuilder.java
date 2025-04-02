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

package com.alibaba.graphscope.common.ir.runtime.write;

import com.alibaba.graphscope.common.ir.meta.schema.GraphOptTable;
import com.alibaba.graphscope.common.ir.rel.ddl.GraphTableModify;
import com.alibaba.graphscope.common.ir.rel.type.FieldMappings;
import com.alibaba.graphscope.common.ir.rel.type.TargetGraph;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.interactive.models.EdgeRequest;
import com.alibaba.graphscope.interactive.models.Property;
import com.alibaba.graphscope.interactive.models.VertexEdgeRequest;
import com.alibaba.graphscope.interactive.models.VertexRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

public class RequestBuilder extends PhysicalBuilder {

    public RequestBuilder(LogicalPlan logicalPlan) {
        super(logicalPlan);
    }

    public PhysicalPlan build() {
        Preconditions.checkArgument(
                logicalPlan.getMode() == LogicalPlan.Mode.WRITE_ONLY,
                "plan mode "
                        + logicalPlan.getMode()
                        + " is not supported in write request builder");
        List<Request> requests = Lists.newArrayList();
        RelVisitor visitor =
                new RelVisitor() {
                    @Override
                    public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                        node.childrenAccept(this);
                        if (node instanceof GraphTableModify) {
                            addWriteOp((GraphTableModify) node, requests);
                        }
                    }
                };
        visitor.go(logicalPlan.getRegularQuery());
        return new PhysicalPlan(requests, requests.toString());
    }

    private void addWriteOp(GraphTableModify writeOp, List<Request> requests) {
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
        } else {
            throw new UnsupportedOperationException(
                    "write op type " + type + " is not supported in request builder");
        }
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
                                    Utils.getValuesAsList(((RexLiteral) source).getValue()).get(0);
                            RexNode target = e.getTarget();
                            Preconditions.checkArgument(
                                    target instanceof RexGraphVariable
                                            && ((RexGraphVariable) target).getProperty() != null);
                            GraphProperty property = ((RexGraphVariable) target).getProperty();
                            ImmutableBitSet propertyIds =
                                    Utils.getPropertyIds(property, schemaType);
                            if (optTable.isKey(propertyIds)) {
                                String propertyName =
                                        ((RexGraphVariable) target).getName().split("\\.")[1];
                                request.setPrimaryKeyValues(
                                        Arrays.asList(
                                                new Property().name(propertyName).value(value)));
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

        if (srcVertex.getPrimaryKeyValues() != null) {
            request.setSrcPrimaryKeyValues(srcVertex.getPrimaryKeyValues());
        }

        if (dstVertex.getPrimaryKeyValues() != null) {
            request.setDstPrimaryKeyValues(dstVertex.getPrimaryKeyValues());
        }

        FieldMappings mappings = edge.getMappings();
        mappings.getMappings()
                .forEach(
                        e -> {
                            RexNode source = e.getSource();
                            Preconditions.checkArgument(source instanceof RexLiteral);
                            Object value =
                                    Utils.getValuesAsList(((RexLiteral) source).getValue()).get(0);
                            RexNode target = e.getTarget();
                            Preconditions.checkArgument(
                                    target instanceof RexGraphVariable
                                            && ((RexGraphVariable) target).getProperty() != null);
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
            GraphSchemaType schemaType = targetGraph.getSingleSchemaType();
            if (schemaType.getScanOpt() == GraphOpt.Source.VERTEX) {
                return Type.ADD_VERTEX;
            } else if (schemaType.getScanOpt() == GraphOpt.Source.EDGE) {
                return Type.ADD_EDGE;
            }
        }
        throw new IllegalArgumentException("unsupported write operation: " + writeOp);
    }

    @Override
    public void close() throws Exception {}
}
