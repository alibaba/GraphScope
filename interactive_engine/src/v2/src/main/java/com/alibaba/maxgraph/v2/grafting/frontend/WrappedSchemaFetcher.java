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
package com.alibaba.maxgraph.v2.grafting.frontend;

import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SnapshotSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WrappedSchemaFetcher implements SchemaFetcher {
    private static final Logger logger = LoggerFactory.getLogger(WrappedSchemaFetcher.class);

    private com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher v2Fetcher;
    private MetaService metaService;

    public WrappedSchemaFetcher(com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaFetcher v2Fetcher,
                                MetaService metaService) {
        this.v2Fetcher = v2Fetcher;
        this.metaService = metaService;
    }

    @Override
    public Pair<GraphSchema, Long> getSchemaSnapshotPair() {
        SnapshotSchema snapshotSchema = this.v2Fetcher.fetchSchema();
        long snapshotId = snapshotSchema.getSnapshotId();
        com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema schema = snapshotSchema.getSchema();
        logger.debug("fetch schema of snapshot id [" + snapshotId + "]");
        return Pair.of(new WrappedSchema(schema), snapshotId);
    }

    @Override
    public int getPartitionNum() {
        return this.metaService.getPartitionCount();
    }

    @Override
    public int getVersion() {
        throw new UnsupportedOperationException();
    }

    private static class WrappedSchema implements GraphSchema {
        private com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema schema;

        public WrappedSchema(com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema schema) {
            this.schema = schema;
        }

        private static GraphProperty fromV2GraphProperty(com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty property) {
            return new GraphProperty() {
                @Override
                public int getId() {
                    return property.getId();
                }

                @Override
                public String getName() {
                    return property.getName();
                }

                @Override
                public PropDataType getDataType() {
                    switch (property.getDataType()) {
                        case BOOL:
                            return PropDataType.BOOL;
                        case CHAR:
                            return PropDataType.CHAR;
                        case SHORT:
                            return PropDataType.SHORT;
                        case INT:
                            return PropDataType.INTEGER;
                        case LONG:
                            return PropDataType.LONG;
                        case FLOAT:
                            return PropDataType.FLOAT;
                        case DOUBLE:
                            return PropDataType.DOUBLE;
                        case STRING:
                            return PropDataType.STRING;
                        case BYTES:
                            return PropDataType.BINARY;
                        case INT_LIST:
                            return PropDataType.INTEGER_LIST;
                        case LONG_LIST:
                            return PropDataType.LONG_LIST;
                        case FLOAT_LIST:
                            return PropDataType.FLOAT_LIST;
                        case DOUBLE_LIST:
                            return PropDataType.DOUBLE_LIST;
                        case STRING_LIST:
                            return PropDataType.STRING_LIST;
                        default:
                            throw new IllegalArgumentException("invalid data type [" + property.getDataType() + "]");
                    }
                }
            };
        }

        private static GraphElement fromSchemaElement(SchemaElement schemaElement) {
            return new GraphElement() {
                @Override
                public String getLabel() {
                    return schemaElement.getLabel();
                }

                @Override
                public int getLabelId() {
                    return schemaElement.getLabelId();
                }

                @Override
                public List<GraphProperty> getPropertyList() {
                    List<com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty> propertyList = schemaElement.getPropertyList();
                    List<GraphProperty> properties = new ArrayList<>(propertyList.size());
                    for (com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty property : propertyList) {
                        properties.add(fromV2GraphProperty(property));
                    }
                    return properties;
                }

                @Override
                public GraphProperty getProperty(int propId) {
                    return fromV2GraphProperty(schemaElement.getProperty(propId));
                }

                @Override
                public GraphProperty getProperty(String propName) {
                    return fromV2GraphProperty(schemaElement.getProperty(propName));
                }
            };
        }

        @Override
        public GraphElement getElement(String label) throws GraphElementNotFoundException {
            return fromSchemaElement(this.schema.getSchemaElement(label));
        }

        @Override
        public GraphElement getElement(int labelId) throws GraphElementNotFoundException {
            return fromSchemaElement(this.schema.getSchemaElement(labelId));
        }

        @Override
        public List<GraphVertex> getVertexList() {
            List<VertexType> vertexTypes = this.schema.getVertexTypes();
            List<GraphVertex> res = new ArrayList<>(vertexTypes.size());
            for (VertexType vertexType : vertexTypes) {
                res.add(fromVertexType(vertexType));
            }
            return res;
        }

        private static GraphVertex fromVertexType(VertexType vertexType) {
            return new GraphVertex() {
                @Override
                public List<GraphProperty> getPrimaryKeyList() {
                    List<String> primaryKeyList = vertexType.getPrimaryKeyConstraint().getPrimaryKeyList();
                    List<GraphProperty> properties = new ArrayList<>(primaryKeyList.size());
                    for (String key : primaryKeyList) {
                        properties.add(fromV2GraphProperty(vertexType.getProperty(key)));
                    }
                    return properties;
                }

                @Override
                public String getLabel() {
                    return vertexType.getLabel();
                }

                @Override
                public int getLabelId() {
                    return vertexType.getLabelId();
                }

                @Override
                public List<GraphProperty> getPropertyList() {
                    List<GraphProperty> res = new ArrayList<>();
                    for (com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty property : vertexType.getPropertyList()) {
                        res.add(fromV2GraphProperty(property));
                    }
                    return res;
                }

                @Override
                public GraphProperty getProperty(int propId) {
                    return fromV2GraphProperty(vertexType.getProperty(propId));
                }

                @Override
                public GraphProperty getProperty(String propName) {
                    return fromV2GraphProperty(vertexType.getProperty(propName));
                }
            };
        }

        @Override
        public List<GraphEdge> getEdgeList() {
            List<EdgeType> edgeTypes = this.schema.getEdgeTypes();
            List<GraphEdge> res = new ArrayList<>(edgeTypes.size());
            for (EdgeType edgeType : edgeTypes) {
                res.add(new GraphEdge() {
                    @Override
                    public List<EdgeRelation> getRelationList() {
                        List<com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation> relationList = edgeType.getRelationList();
                        List<EdgeRelation> relations = new ArrayList<>(relationList.size());
                        for (com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation relation : relationList) {
                            relations.add(new EdgeRelation() {
                                @Override
                                public GraphVertex getSource() {
                                    return fromVertexType(relation.getSource());
                                }

                                @Override
                                public GraphVertex getTarget() {
                                    return fromVertexType(relation.getTarget());
                                }
                            });
                        }
                        return relations;
                    }

                    @Override
                    public String getLabel() {
                        return edgeType.getLabel();
                    }

                    @Override
                    public int getLabelId() {
                        return edgeType.getLabelId();
                    }

                    @Override
                    public List<GraphProperty> getPropertyList() {
                        List<GraphProperty> res = new ArrayList<>();
                        for (com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty property : edgeType.getPropertyList()) {
                            res.add(fromV2GraphProperty(property));
                        }
                        return res;
                    }

                    @Override
                    public GraphProperty getProperty(int propId) {
                        return fromV2GraphProperty(edgeType.getProperty(propId));
                    }

                    @Override
                    public GraphProperty getProperty(String propName) {
                        return fromV2GraphProperty(edgeType.getProperty(propName));
                    }
                });
            }
            return res;
        }

        @Override
        public Integer getPropertyId(String propName) throws GraphPropertyNotFoundException {
            return this.schema.getPropertyId(propName).values().iterator().next();
        }

        @Override
        public String getPropertyName(int propId) throws GraphPropertyNotFoundException {
            return this.schema.getPropertyName(propId).values().iterator().next();
        }

        @Override
        public Map<GraphElement, GraphProperty> getPropertyList(String propName) {
            Map<Integer, com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty> propertyDefinitions = this.schema.getPropertyDefinitions(propName);
            Map<GraphElement, GraphProperty> res = new HashMap<>();
            propertyDefinitions.forEach((id, def) -> {
                GraphElement graphElement = fromSchemaElement(this.schema.getSchemaElement(id));
                GraphProperty graphProperty = fromV2GraphProperty(def);
                res.put(graphElement, graphProperty);
            });
            return res;
        }

        @Override
        public Map<GraphElement, GraphProperty> getPropertyList(int propId) {
            Map<GraphElement, GraphProperty> res = new HashMap<>();
            for (String labelName : this.schema.getPropertyName(propId).keySet()) {
                SchemaElement schemaElement = this.schema.getSchemaElement(labelName);
                int labelId = schemaElement.getLabelId();
                res.put(fromSchemaElement(schemaElement), fromV2GraphProperty(this.schema.getPropertyDefinition(labelId, propId)));
            }
            return res;
        }
    }
}
