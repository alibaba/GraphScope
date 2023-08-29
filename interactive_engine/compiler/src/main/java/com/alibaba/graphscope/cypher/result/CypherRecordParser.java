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

package com.alibaba.graphscope.cypher.result;

import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CypherRecordParser implements RecordParser<AnyValue> {
    private static final Logger logger = LoggerFactory.getLogger(CypherRecordParser.class);
    private final RelDataType outputType;

    public CypherRecordParser(RelDataType outputType) {
        this.outputType = outputType;
    }

    @Override
    public List<AnyValue> parseFrom(IrResult.Record record) {
        logger.debug("record {}", record);
        Preconditions.checkArgument(
                record.getColumnsCount() == outputType.getFieldCount(),
                "column size of results "
                        + record.getColumnsCount()
                        + " should be consistent with output type "
                        + outputType.getFieldCount());
        List<AnyValue> columns = new ArrayList<>(record.getColumnsCount());
        for (int i = 0; i < record.getColumnsCount(); i++) {
            IrResult.Column column = record.getColumns(i);
            RelDataTypeField field = outputType.getFieldList().get(i);
            columns.add(parseEntry(column.getEntry(), field.getType()));
        }
        return columns;
    }

    @Override
    public RelDataType schema() {
        return this.outputType;
    }

    protected AnyValue parseEntry(IrResult.Entry entry, @Nullable RelDataType dataType) {
        if (dataType instanceof GraphPathType) {
            return parseElement(entry.getElement(), dataType);
        }
        switch (dataType.getSqlTypeName()) {
            case MULTISET:
            case ARRAY:
                return parseCollection(entry.getCollection(), dataType.getComponentType());
            default:
                return parseElement(entry.getElement(), dataType);
        }
    }

    protected AnyValue parseElement(IrResult.Element element, @Nullable RelDataType dataType) {
        switch (element.getInnerCase()) {
            case VERTEX:
                return parseVertex(element.getVertex(), dataType);
            case EDGE:
                return parseEdge(element.getEdge(), dataType);
            case GRAPH_PATH:
                return parseGraphPath(element.getGraphPath(), dataType);
            case OBJECT:
            default:
                return parseValue(element.getObject(), dataType);
        }
    }

    protected AnyValue parseCollection(
            IrResult.Collection collection, @Nullable RelDataType componentType) {
        switch (componentType.getSqlTypeName()) {
            case BOOLEAN:
                Boolean[] boolObjs =
                        collection.getCollectionList().stream()
                                .map(k -> k.getObject().getBoolean())
                                .toArray(Boolean[]::new);
                return Values.booleanArray(ArrayUtils.toPrimitive(boolObjs));
            case INTEGER:
                return Values.intArray(
                        collection.getCollectionList().stream()
                                .mapToInt(k -> k.getObject().getI32())
                                .toArray());
            case BIGINT:
                return Values.longArray(
                        collection.getCollectionList().stream()
                                .mapToLong(k -> k.getObject().getI64())
                                .toArray());
            case DOUBLE:
                return Values.doubleArray(
                        collection.getCollectionList().stream()
                                .mapToDouble(k -> k.getObject().getF64())
                                .toArray());
            case CHAR:
                return Values.stringArray(
                        collection.getCollectionList().stream()
                                .map(k -> k.getObject().getStr())
                                .toArray(String[]::new));
            case ROW:
            case ANY:
                return VirtualValues.fromList(
                        collection.getCollectionList().stream()
                                .map(k -> parseElement(k, componentType))
                                .collect(Collectors.toList()));
            default:
                throw new NotImplementedException(
                        componentType.getSqlTypeName() + " is unsupported yet");
        }
    }

    protected NodeValue parseVertex(IrResult.Vertex vertex, @Nullable RelDataType dataType) {
        return VirtualValues.nodeValue(
                vertex.getId(),
                Values.stringArray(getLabelName(vertex.getLabel(), getLabelTypes(dataType))),
                MapValue.EMPTY);
    }

    protected RelationshipValue parseEdge(IrResult.Edge edge, @Nullable RelDataType dataType) {
        return VirtualValues.relationshipValue(
                edge.getId(),
                VirtualValues.nodeValue(
                        edge.getSrcId(),
                        Values.stringArray(
                                getSrcLabelName(edge.getSrcLabel(), getLabelTypes(dataType))),
                        MapValue.EMPTY),
                VirtualValues.nodeValue(
                        edge.getDstId(),
                        Values.stringArray(
                                getDstLabelName(edge.getDstLabel(), getLabelTypes(dataType))),
                        MapValue.EMPTY),
                Values.stringValue(getLabelName(edge.getLabel(), getLabelTypes(dataType))),
                MapValue.EMPTY);
    }

    protected AnyValue parseGraphPath(IrResult.GraphPath path, @Nullable RelDataType dataType) {
        List<NodeValue> nodes = Lists.newArrayList();
        List<RelationshipValue> relationships = Lists.newArrayList();
        path.getPathList()
                .forEach(
                        k -> {
                            switch (k.getInnerCase()) {
                                case VERTEX:
                                    nodes.add(parseVertex(k.getVertex(), getVertexType(dataType)));
                                    break;
                                case EDGE:
                                    relationships.add(
                                            parseEdge(k.getEdge(), getEdgeType(dataType)));
                                    break;
                            }
                        });
        return VirtualValues.path(
                nodes.toArray(NodeValue[]::new), relationships.toArray(RelationshipValue[]::new));
    }

    protected AnyValue parseValue(Common.Value value, @Nullable RelDataType dataType) {
        switch (value.getItemCase()) {
            case BOOLEAN:
                return value.getBoolean() ? BooleanValue.TRUE : BooleanValue.FALSE;
            case I32:
                return Values.intValue(value.getI32());
            case I64:
                return Values.longValue(value.getI64());
            case F64:
                return Values.doubleValue(value.getF64());
            case STR:
                return Values.stringValue(value.getStr());
            case I32_ARRAY:
                return Values.intArray(
                        value.getI32Array().getItemList().stream()
                                .mapToInt(k -> k.intValue())
                                .toArray());
            case I64_ARRAY:
                return Values.longArray(
                        value.getI64Array().getItemList().stream()
                                .mapToLong(k -> k.longValue())
                                .toArray());
            case F64_ARRAY:
                return Values.doubleArray(
                        value.getF64Array().getItemList().stream()
                                .mapToDouble(k -> k.doubleValue())
                                .toArray());
            case STR_ARRAY:
                return Values.stringArray(value.getStrArray().getItemList().toArray(String[]::new));
            case NONE:
                return Values.NO_VALUE;
            default:
                throw new NotImplementedException(value.getItemCase() + " is unsupported yet");
        }
    }

    private String getLabelName(Common.NameOrId nameOrId, List<GraphLabelType> labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                for (GraphLabelType labelType : labelTypes) {
                    if (labelType.getLabelId() == nameOrId.getId()) {
                        return labelType.getLabel();
                    }
                    labelIds.add(labelType.getLabelId());
                }
                logger.warn(
                        "label id={} not found, expected ids are {}", nameOrId.getId(), labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    private String getSrcLabelName(Common.NameOrId nameOrId, List<GraphLabelType> labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                for (GraphLabelType labelType : labelTypes) {
                    if (labelType.getSrcLabelId() == nameOrId.getId()) {
                        return labelType.getSrcLabel();
                    }
                }
                logger.warn(
                        "src label id={} not found, expected ids are {}",
                        nameOrId.getId(),
                        labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    private String getDstLabelName(Common.NameOrId nameOrId, List<GraphLabelType> labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                for (GraphLabelType labelType : labelTypes) {
                    if (labelType.getDstLabelId() == nameOrId.getId()) {
                        return labelType.getDstLabel();
                    }
                }
                logger.warn(
                        "dst label id={} not found, expected ids are {}",
                        nameOrId.getId(),
                        labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    private List<GraphLabelType> getLabelTypes(RelDataType dataType) {
        List<GraphLabelType> labelTypes = Lists.newArrayList();
        if (dataType instanceof GraphSchemaTypeList) {
            ((GraphSchemaTypeList) dataType)
                    .forEach(
                            k -> {
                                labelTypes.add(k.getLabelType());
                            });
        } else if (dataType instanceof GraphSchemaType) {
            labelTypes.add(((GraphSchemaType) dataType).getLabelType());
        }
        return labelTypes;
    }

    private RelDataType getVertexType(RelDataType graphPathType) {
        return (graphPathType instanceof GraphPathType)
                ? ((GraphPathType) graphPathType).getComponentType().getGetVType()
                : graphPathType;
    }

    private RelDataType getEdgeType(RelDataType graphPathType) {
        return (graphPathType instanceof GraphPathType)
                ? ((GraphPathType) graphPathType).getComponentType().getExpandType()
                : graphPathType;
    }
}
