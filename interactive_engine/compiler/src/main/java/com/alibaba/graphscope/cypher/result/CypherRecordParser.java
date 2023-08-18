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
import com.alibaba.graphscope.common.ir.type.GraphPxdElementType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.*;
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

    protected NodeValue parseVertex(IrResult.Vertex vertex, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType,
                "data type of vertex should be " + GraphSchemaType.class);
        return VirtualValues.nodeValue(
                vertex.getId(),
                Values.stringArray(
                        getLabelName(vertex.getLabel(), getLabelTypes((GraphSchemaType) dataType))),
                MapValue.EMPTY);
    }

    protected RelationshipValue parseEdge(IrResult.Edge edge, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType,
                "data type of edge should be " + GraphSchemaType.class);
        return VirtualValues.relationshipValue(
                edge.getId(),
                VirtualValues.nodeValue(
                        edge.getSrcId(),
                        Values.stringArray(
                                getSrcLabelName(
                                        edge.getSrcLabel(),
                                        getLabelTypes((GraphSchemaType) dataType))),
                        MapValue.EMPTY),
                VirtualValues.nodeValue(
                        edge.getDstId(),
                        Values.stringArray(
                                getDstLabelName(
                                        edge.getDstLabel(),
                                        getLabelTypes((GraphSchemaType) dataType))),
                        MapValue.EMPTY),
                Values.stringValue(
                        getLabelName(edge.getLabel(), getLabelTypes((GraphSchemaType) dataType))),
                MapValue.EMPTY);
    }

    protected AnyValue parseGraphPath(IrResult.GraphPath path, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(dataType.getSqlTypeName() == SqlTypeName.ARRAY);
        ArraySqlType arrayType = (ArraySqlType) dataType;
        Preconditions.checkArgument(arrayType.getComponentType() instanceof GraphPxdElementType);
        // todo: support path expand result
        GraphPxdElementType elementType = (GraphPxdElementType) arrayType.getComponentType();
        throw new NotImplementedException("type " + PathValue.class + " is not implemented yet");
    }

    protected AnyValue parseValue(Common.Value value, @Nullable RelDataType dataType) {
        switch (dataType.getSqlTypeName()) {
            case BOOLEAN:
                return value.getBoolean() ? BooleanValue.TRUE : BooleanValue.FALSE;
            case INTEGER:
                return Values.intValue(value.getI32());
            case BIGINT:
                return Values.longValue(value.getI64());
            case DOUBLE:
                return Values.doubleValue(value.getF64());
            case CHAR:
                return Values.stringValue(value.getStr());
            case ARRAY:
                // todo: support array of any component type in ir core
                return Values.stringArray(value.getStrArray().getItemList().toArray(new String[0]));
            default:
                throw new NotImplementedException(value.getItemCase() + " is unsupported yet");
        }
    }

    protected AnyValue parseCollection(
            IrResult.Collection collection, @Nullable RelDataType dataType) {
        // multiset is the data type of aggregate function collect
        // array is the data type of path collection
        Preconditions.checkArgument(
                dataType.getSqlTypeName() == SqlTypeName.MULTISET
                        || dataType.getSqlTypeName() == SqlTypeName.ARRAY);
        switch (dataType.getComponentType().getSqlTypeName()) {
            case BOOLEAN:
                return Values.booleanArray(
                        convert(
                                collection.getCollectionList().stream()
                                        .map(k -> k.getObject().getBoolean())
                                        .collect(Collectors.toList())));
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
            default:
                throw new NotImplementedException(
                        dataType.getComponentType().getSqlTypeName() + " is unsupported yet");
        }
    }

    private boolean[] convert(List<Boolean> values) {
        boolean[] result = new boolean[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    protected AnyValue parseEntry(IrResult.Entry entry, @Nullable RelDataType dataType) {
        switch (entry.getInnerCase()) {
            case ELEMENT:
                return parseElement(entry.getElement(), dataType);
            case COLLECTION:
            default:
                return parseCollection(entry.getCollection(), dataType);
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
                throw new IllegalArgumentException(
                        "label id="
                                + nameOrId.getId()
                                + " not found, expected ids are "
                                + labelIds);
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
                throw new IllegalArgumentException(
                        "src label id="
                                + nameOrId.getId()
                                + " not found, expected ids are "
                                + labelIds);
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
                throw new IllegalArgumentException(
                        "dst label id="
                                + nameOrId.getId()
                                + " not found, expected ids are "
                                + labelIds);
        }
    }

    private List<GraphLabelType> getLabelTypes(GraphSchemaType dataType) {
        List<GraphLabelType> labelTypes = new ArrayList<>();
        if (dataType instanceof GraphSchemaTypeList) {
            ((GraphSchemaTypeList) dataType)
                    .forEach(
                            k -> {
                                labelTypes.add(k.getLabelType());
                            });
        } else {
            labelTypes.add(dataType.getLabelType());
        }
        return labelTypes;
    }
}
