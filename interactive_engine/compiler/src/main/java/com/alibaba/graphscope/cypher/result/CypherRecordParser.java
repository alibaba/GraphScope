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
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CypherRecordParser implements RecordParser<AnyValue> {
    private static final Logger logger = LoggerFactory.getLogger(CypherRecordParser.class);
    private final RelDataType outputType;

    public CypherRecordParser(RelDataType outputType) {
        this.outputType = outputType;
    }

    @Override
    public List<AnyValue> parseFrom(IrResult.Record record) {
        logger.info("record: {}, expected type {}", record, this.outputType);
        Preconditions.checkArgument(record.getColumnsCount() == outputType.getFieldCount(), "column size of results should be consistent with output type");
        List<AnyValue> columns = new ArrayList<>(record.getColumnsCount());
        for (int i = 0; i < record.getColumnsCount(); i++) {
            IrResult.Column column = record.getColumns(i);
            RelDataTypeField field = outputType.getFieldList().get(i);
            logger.info("column: {}, field {}", column, field);
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
                Values.stringArray(getLabelName(vertex.getLabel(), getLabelTypes((GraphSchemaType) dataType))),
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
                        Values.stringArray(getSrcLabelName(edge.getSrcLabel(), getLabelTypes((GraphSchemaType) dataType))),
                        MapValue.EMPTY),
                VirtualValues.nodeValue(
                        edge.getDstId(),
                        Values.stringArray(getDstLabelName(edge.getDstLabel(), getLabelTypes((GraphSchemaType) dataType))),
                        MapValue.EMPTY),
                Values.stringValue(getLabelName(edge.getLabel(), getLabelTypes((GraphSchemaType) dataType))),
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
            case NONE:
                return Values.NO_VALUE;
            case PAIR_ARRAY:
            case STR_ARRAY:
            case I32_ARRAY:
            case F64_ARRAY:
            default:
                throw new GremlinResultParserException(value.getItemCase() + " is unsupported yet");
        }
    }

    protected AnyValue parseCollection(
            IrResult.Collection collection, @Nullable RelDataType dataType) {
        // multiset is the data type of aggregate function collect
        // array is the data type of path collection
        Preconditions.checkArgument(
                dataType.getSqlTypeName() == SqlTypeName.MULTISET
                        || dataType.getSqlTypeName() == SqlTypeName.ARRAY);
        // todo: support collect result
        throw new NotImplementedException("type " + ArrayValue.class + " is not implemented yet");
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
