/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.exception.GraphElementNotFoundException;
import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.*;
import com.alibaba.maxgraph.proto.groot.*;
import com.alibaba.maxgraph.compiler.api.exception.InvalidSchemaException;
import com.alibaba.maxgraph.compiler.api.exception.TypeDefNotFoundException;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GraphDef implements GraphSchema {

    private long version;
    private Map<String, LabelId> labelToId;
    private Map<LabelId, TypeDef> idToType;
    private Map<LabelId, Set<EdgeKind>> idToKinds;
    private Map<String, Integer> propertyNameToId;
    private int labelIdx;
    private int propertyIdx;
    private Map<LabelId, Long> vertexTableIds;
    private Map<EdgeKind, Long> edgeTableIds;
    private long tableIdx;

    private Map<LabelId, GraphVertex> vertexTypes;
    private Map<LabelId, GraphEdge> edgeTypes;

    public GraphDef(
            long version,
            Map<String, LabelId> labelToId,
            Map<LabelId, TypeDef> idToType,
            Map<LabelId, Set<EdgeKind>> idToKinds,
            Map<String, Integer> propertyNameToId,
            int labelIdx,
            int propertyIdx,
            Map<LabelId, Long> vertexTableIds,
            Map<EdgeKind, Long> edgeTableIds,
            long tableIdx) {
        this.version = version;
        this.labelToId = Collections.unmodifiableMap(new HashMap<>(labelToId));
        this.idToType = Collections.unmodifiableMap(new HashMap<>(idToType));
        this.idToKinds = Collections.unmodifiableMap(new HashMap<>(idToKinds));
        this.propertyNameToId = Collections.unmodifiableMap(new HashMap<>(propertyNameToId));
        this.vertexTableIds = Collections.unmodifiableMap(new HashMap<>(vertexTableIds));
        this.edgeTableIds = Collections.unmodifiableMap(new HashMap<>(edgeTableIds));

        this.labelIdx = labelIdx;
        this.propertyIdx = propertyIdx;
        this.tableIdx = tableIdx;

        buildVertexTypes();
        buildEdgeTypes();
    }

    private void buildEdgeTypes() {
        this.edgeTypes = new HashMap<>();
        for (TypeDef typeDef : this.idToType.values()) {
            if (typeDef.getTypeEnum() == TypeEnum.EDGE) {
                LabelId labelId = typeDef.getTypeLabelId();
                GraphEdgeImpl edgeType = getEdgeType(typeDef);
                this.edgeTypes.put(labelId, edgeType);
            }
        }
    }

    private GraphEdgeImpl getEdgeType(TypeDef typeDef) {
        LabelId labelId = typeDef.getTypeLabelId();
        Set<EdgeKind> edgeKindSet = this.idToKinds.get(labelId);
        List<EdgeRelation> edgeRelations = new ArrayList<>();
        if (edgeKindSet != null) {
            for (EdgeKind edgeKind : edgeKindSet) {
                GraphVertex srcGraphVertex = this.vertexTypes.get(edgeKind.getSrcVertexLabelId());
                GraphVertex dstGraphVertex = this.vertexTypes.get(edgeKind.getDstVertexLabelId());
                Long tableId = edgeTableIds.get(edgeKind);
                if (tableId == null) {
                    throw new InvalidSchemaException("no valid table id for [" + edgeKind + "]");
                }
                edgeRelations.add(new EdgeRelationImpl(tableId, srcGraphVertex, dstGraphVertex));
            }
        }
        return new GraphEdgeImpl(typeDef, edgeRelations);
    }

    private void buildVertexTypes() {
        this.vertexTypes = new HashMap<>();
        for (TypeDef typeDef : this.idToType.values()) {
            if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
                LabelId labelId = typeDef.getTypeLabelId();
                GraphVertexImpl vertexType = getVertexType(typeDef);
                this.vertexTypes.put(labelId, vertexType);
            }
        }
    }

    private GraphVertexImpl getVertexType(TypeDef typeDef) {
        LabelId labelId = typeDef.getTypeLabelId();
        Long tableId = vertexTableIds.get(labelId);
        if (tableId == null) {
            throw new InvalidSchemaException("no valid table id for [" + typeDef.getLabel() + "]");
        }
        return new GraphVertexImpl(typeDef, tableId);
    }

    public static GraphDef parseProto(GraphDefPb proto) {
        long version = proto.getVersion();
        Map<String, LabelId> labelToId = new HashMap<>();
        Map<LabelId, TypeDef> idToType = new HashMap<>();
        for (TypeDefPb typeDefPb : proto.getTypeDefsList()) {
            TypeDef typeDef = TypeDef.parseProto(typeDefPb);
            LabelId labelId = typeDef.getTypeLabelId();
            labelToId.put(typeDef.getLabel(), labelId);
            idToType.put(labelId, typeDef);
        }
        Map<LabelId, Set<EdgeKind>> idToKinds = new HashMap<>();
        for (EdgeKindPb edgeKindPb : proto.getEdgeKindsList()) {
            EdgeKind edgeKind = EdgeKind.parseProto(edgeKindPb);
            Set<EdgeKind> edgeKindSet =
                    idToKinds.computeIfAbsent(edgeKind.getEdgeLabelId(), k -> new HashSet<>());
            edgeKindSet.add(edgeKind);
        }
        Map<String, Integer> propertyNameToId = proto.getPropertyNameToIdMap();
        int labelIdx = proto.getLabelIdx();
        int propertyIdx = proto.getPropertyIdx();
        Map<LabelId, Long> vertexTableIds = new HashMap<>();
        for (VertexTableIdEntry vertexTableIdEntry : proto.getVertexTableIdsList()) {
            vertexTableIds.put(
                    LabelId.parseProto(vertexTableIdEntry.getLabelId()),
                    vertexTableIdEntry.getTableId());
        }
        Map<EdgeKind, Long> edgeTableIds = new HashMap<>();
        for (EdgeTableIdEntry edgeTableIdEntry : proto.getEdgeTableIdsList()) {
            edgeTableIds.put(
                    EdgeKind.parseProto(edgeTableIdEntry.getEdgeKind()),
                    edgeTableIdEntry.getTableId());
        }
        long tableIdx = proto.getTableIdx();
        return new GraphDef(
                version,
                labelToId,
                idToType,
                idToKinds,
                propertyNameToId,
                labelIdx,
                propertyIdx,
                vertexTableIds,
                edgeTableIds,
                tableIdx);
    }

    public GraphDefPb toProto() {
        GraphDefPb.Builder builder = GraphDefPb.newBuilder();
        builder.setVersion(version);
        for (TypeDef typeDef : idToType.values()) {
            builder.addTypeDefs(typeDef.toProto());
        }
        for (Set<EdgeKind> edgeKinds : idToKinds.values()) {
            for (EdgeKind edgeKind : edgeKinds) {
                builder.addEdgeKinds(edgeKind.toProto());
            }
        }
        builder.putAllPropertyNameToId(propertyNameToId);
        builder.setLabelIdx(labelIdx);
        builder.setPropertyIdx(propertyIdx);
        vertexTableIds.forEach(
                (k, v) -> {
                    builder.addVertexTableIds(
                            VertexTableIdEntry.newBuilder()
                                    .setLabelId(k.toProto())
                                    .setTableId(v)
                                    .build());
                });
        edgeTableIds.forEach(
                (k, v) -> {
                    builder.addEdgeTableIds(
                            EdgeTableIdEntry.newBuilder()
                                    .setEdgeKind(k.toProto())
                                    .setTableId(v)
                                    .build());
                });
        builder.setTableIdx(tableIdx);
        return builder.build();
    }

    public TypeDef getTypeDef(String label) {
        LabelId labelId = this.labelToId.get(label);
        if (labelId == null) {
            throw new TypeDefNotFoundException("no such label [" + label + "]");
        }
        return getTypeDef(labelId);
    }

    public TypeDef getTypeDef(LabelId labelId) {
        return this.idToType.get(labelId);
    }

    public boolean hasLabel(String label) {
        return this.labelToId.containsKey(label);
    }

    public LabelId getLabelId(String label) {
        return this.labelToId.get(label);
    }

    public boolean hasEdgeKind(EdgeKind edgeKind) {
        Set<EdgeKind> edgeKindSet = this.idToKinds.get(edgeKind.getEdgeLabelId());
        if (edgeKindSet == null) {
            return false;
        }
        return edgeKindSet.contains(edgeKind);
    }

    @Override
    public GraphElement getElement(String label) throws GraphElementNotFoundException {
        LabelId labelId = labelToId.get(label);
        if (labelId == null) {
            throw new GraphElementNotFoundException("schema element not found for label " + label);
        }
        return convertVertexEdgeType(idToType.get(labelId));
    }

    private GraphElement convertVertexEdgeType(TypeDef typeDef) {
        if (null == typeDef) {
            throw new RuntimeException("No type def with given label id/name");
        }
        if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
            return getVertexType(typeDef);
        } else if (typeDef.getTypeEnum() == TypeEnum.EDGE) {
            return getEdgeType(typeDef);
        } else {
            throw new IllegalArgumentException("Not support type value " + typeDef.getTypeEnum());
        }
    }

    @Override
    public GraphElement getElement(int labelId) throws GraphElementNotFoundException {
        TypeDef typeDef = idToType.get(new LabelId(labelId));
        return convertVertexEdgeType(typeDef);
    }

    @Override
    public List<GraphVertex> getVertexList() {
        return new ArrayList<>(this.vertexTypes.values());
    }

    @Override
    public List<GraphEdge> getEdgeList() {
        return new ArrayList<>(this.edgeTypes.values());
    }

    @Override
    public Integer getPropertyId(String propertyName) throws GraphPropertyNotFoundException {
        if (propertyNameToId.containsKey(propertyName)) {
            return propertyNameToId.get(propertyName);
        }

        throw new GraphPropertyNotFoundException("property " + propertyName + " not exist");
    }

    @Override
    public String getPropertyName(int propertyId) throws GraphPropertyNotFoundException {
        for (Map.Entry<String, Integer> entry : propertyNameToId.entrySet()) {
            if (entry.getValue() == propertyId) {
                return entry.getKey();
            }
        }
        throw new GraphPropertyNotFoundException(
                "property not exist for property id " + propertyId);
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(String propertyName) {
        Map<GraphElement, GraphProperty> elementToProperty = new HashMap<>();
        for (TypeDef typeDef : this.idToType.values()) {
            GraphProperty property = typeDef.getProperty(propertyName);
            if (property == null) {
                continue;
            }
            elementToProperty.put(typeDef, property);
        }
        return elementToProperty;
    }

    @Override
    public Map<GraphElement, GraphProperty> getPropertyList(int propId) {
        String propertyName = getPropertyName(propId);
        return getPropertyList(propertyName);
    }

    @Override
    public int getVersion() {
        return (int) version;
    }

    public long getSchemaVersion() {
        return version;
    }

    public Map<String, LabelId> getLabelToId() {
        return labelToId;
    }

    public Map<LabelId, TypeDef> getIdToType() {
        return idToType;
    }

    public Map<LabelId, Set<EdgeKind>> getIdToKinds() {
        return idToKinds;
    }

    public int getLabelIdx() {
        return labelIdx;
    }

    public int getPropertyIdx() {
        return propertyIdx;
    }

    public Map<String, Integer> getPropertyNameToId() {
        return propertyNameToId;
    }

    public Map<LabelId, Long> getVertexTableIds() {
        return vertexTableIds;
    }

    public Map<EdgeKind, Long> getEdgeTableIds() {
        return edgeTableIds;
    }

    public long getTableIdx() {
        return tableIdx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphDef graphDef = (GraphDef) o;
        return version == graphDef.version
                && labelIdx == graphDef.labelIdx
                && propertyIdx == graphDef.propertyIdx
                && tableIdx == graphDef.tableIdx
                && Objects.equals(labelToId, graphDef.labelToId)
                && Objects.equals(idToType, graphDef.idToType)
                && Objects.equals(idToKinds, graphDef.idToKinds)
                && Objects.equals(propertyNameToId, graphDef.propertyNameToId)
                && Objects.equals(vertexTableIds, graphDef.vertexTableIds)
                && Objects.equals(edgeTableIds, graphDef.edgeTableIds)
                && Objects.equals(vertexTypes, graphDef.vertexTypes)
                && Objects.equals(edgeTypes, graphDef.edgeTypes);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(GraphDef graphDef) {
        return new Builder(graphDef);
    }

    public static class Builder {

        private long version;
        private Map<String, LabelId> labelToId;
        private Map<LabelId, TypeDef> idToType;
        private Map<LabelId, Set<EdgeKind>> idToKinds;
        private Map<String, Integer> propertyNameToId;
        private int labelIdx;
        private int propertyIdx;
        private Map<LabelId, Long> vertexTableIds;
        private Map<EdgeKind, Long> edgeTableIds;
        private long tableIdx;

        private Builder() {
            this.version = 0;
            this.labelToId = new HashMap<>();
            this.idToType = new HashMap<>();
            this.idToKinds = new HashMap<>();
            this.propertyNameToId = new HashMap<>();
            this.labelIdx = 0;
            this.propertyIdx = 0;
            this.vertexTableIds = new HashMap<>();
            this.edgeTableIds = new HashMap<>();
            this.tableIdx = 0L;
        }

        private Builder(GraphDef graphDef) {
            this.version = graphDef.getSchemaVersion();
            this.labelToId = new HashMap<>(graphDef.getLabelToId());
            this.idToType = new HashMap<>(graphDef.getIdToType());
            this.idToKinds = new HashMap<>(graphDef.getIdToKinds());
            this.propertyNameToId = new HashMap<>(graphDef.getPropertyNameToId());
            this.labelIdx = graphDef.getLabelIdx();
            this.propertyIdx = graphDef.getPropertyIdx();
            this.vertexTableIds = new HashMap<>(graphDef.getVertexTableIds());
            this.edgeTableIds = new HashMap<>(graphDef.getEdgeTableIds());
            this.tableIdx = graphDef.getTableIdx();
        }

        public Builder setLabelIdx(int labelIdx) {
            this.labelIdx = labelIdx;
            return this;
        }

        public Builder setPropertyIdx(int propertyIdx) {
            this.propertyIdx = propertyIdx;
            return this;
        }

        public Builder putPropertyNameToId(String propertyName, int id) {
            this.propertyNameToId.put(propertyName, id);
            return this;
        }

        public Builder putVertexTableId(LabelId labelId, long tableId) {
            this.vertexTableIds.put(labelId, tableId);
            return this;
        }

        public Builder putEdgeTableId(EdgeKind edgeKind, long tableId) {
            this.edgeTableIds.put(edgeKind, tableId);
            return this;
        }

        public Builder setTableIdx(long tableIdx) {
            this.tableIdx = tableIdx;
            return this;
        }

        public Builder clearUnusedPropertyName(Set<String> usingPropertyNames) {
            Set<String> removePropertyNames = Sets.newHashSet();
            for (String k : this.propertyNameToId.keySet()) {
                if (!usingPropertyNames.contains(k)) {
                    removePropertyNames.add(k);
                }
            }
            removePropertyNames.forEach(k -> this.propertyNameToId.remove(k));
            return this;
        }

        public Builder addTypeDef(TypeDef typeDef) {
            LabelId labelId = typeDef.getTypeLabelId();
            this.labelToId.put(typeDef.getLabel(), labelId);
            this.idToType.put(labelId, typeDef);
            return this;
        }

        public Builder removeTypeDef(String label) {
            LabelId removedLabelId = this.labelToId.remove(label);
            this.idToType.remove(removedLabelId);
            this.vertexTableIds.remove(removedLabelId);
            return this;
        }

        public Builder addEdgeKind(EdgeKind edgeKind) {
            Set<EdgeKind> edgeKindSet =
                    this.idToKinds.computeIfAbsent(edgeKind.getEdgeLabelId(), k -> new HashSet<>());
            edgeKindSet.add(edgeKind);
            return this;
        }

        public Builder removeEdgeKind(EdgeKind edgeKind) {
            LabelId edgeLabelId = edgeKind.getEdgeLabelId();
            Set<EdgeKind> edgeKindSet = this.idToKinds.get(edgeLabelId);
            if (edgeKindSet == null) {
                return this;
            }
            edgeKindSet.remove(edgeKind);
            this.edgeTableIds.remove(edgeKind);
            if (edgeKindSet.size() == 0) {
                this.idToKinds.remove(edgeLabelId);
            }
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Collection<TypeDef> getAllTypeDefs() {
            return this.idToType.values();
        }

        public GraphDef build() {
            return new GraphDef(
                    version,
                    labelToId,
                    idToType,
                    idToKinds,
                    propertyNameToId,
                    labelIdx,
                    propertyIdx,
                    vertexTableIds,
                    edgeTableIds,
                    tableIdx);
        }
    }
}
