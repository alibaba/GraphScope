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

import com.alibaba.graphscope.common.ir.type.ArbitraryArrayType;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.common.result.Utils;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.*;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                if (dataType instanceof ArbitraryArrayType) {
                    return parseCollection(
                            entry.getCollection(),
                            ((ArbitraryArrayType) dataType).getComponentTypes());
                } else {
                    return parseCollection(entry.getCollection(), dataType.getComponentType());
                }
            case MAP:
                if (dataType instanceof ArbitraryMapType) {
                    return parseKeyValues(
                            entry.getMap(),
                            ((ArbitraryMapType) dataType).getKeyTypes(),
                            ((ArbitraryMapType) dataType).getValueTypes());
                } else {
                    return parseKeyValues(
                            entry.getMap(), dataType.getKeyType(), dataType.getValueType());
                }
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

    protected AnyValue parseCollection(IrResult.Collection collection, RelDataType componentType) {
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
                return VirtualValues.fromList(
                        collection.getCollectionList().stream()
                                .map(k -> parseElement(k, componentType))
                                .collect(Collectors.toList()));
            default:
                throw new NotImplementedException(
                        componentType.getSqlTypeName() + " is unsupported yet");
        }
    }

    protected AnyValue parseCollection(
            IrResult.Collection collection, List<RelDataType> componentTypes) {
        List<IrResult.Element> elements = collection.getCollectionList();
        Preconditions.checkArgument(
                elements.size() == componentTypes.size(),
                "Collection element size="
                        + elements.size()
                        + " is not consistent with type size="
                        + componentTypes.size());
        List<AnyValue> values = Lists.newArrayList();
        for (int i = 0; i < elements.size(); ++i) {
            values.add(parseElement(elements.get(i), componentTypes.get(i)));
        }
        return VirtualValues.fromList(values);
    }

    protected AnyValue parseKeyValues(
            IrResult.KeyValues keyValues, RelDataType keyType, RelDataType valueType) {
        Map<String, AnyValue> valueMap = Maps.newLinkedHashMap();
        keyValues
                .getKeyValuesList()
                .forEach(
                        entry -> {
                            valueMap.put(
                                    entry.getKey().getStr(),
                                    parseElement(entry.getValue(), valueType));
                        });
        return VirtualValues.fromMap(valueMap, valueMap.size(), 0);
    }

    protected AnyValue parseKeyValues(
            IrResult.KeyValues keyValues,
            List<RelDataType> keyTypes,
            List<RelDataType> valueTypes) {
        List<IrResult.KeyValues.KeyValue> entries = keyValues.getKeyValuesList();
        Preconditions.checkArgument(
                entries.size() == valueTypes.size(),
                "KeyValues entry size="
                        + entries.size()
                        + " is not consistent with value type size="
                        + valueTypes.size());
        Map<String, AnyValue> valueMap = Maps.newLinkedHashMap();
        for (int i = 0; i < entries.size(); ++i) {
            IrResult.KeyValues.KeyValue entry = entries.get(i);
            valueMap.put(
                    entry.getKey().getStr(), parseElement(entry.getValue(), valueTypes.get(i)));
        }
        return VirtualValues.fromMap(valueMap, valueMap.size(), 0);
    }

    protected NodeValue parseVertex(IrResult.Vertex vertex, @Nullable RelDataType dataType) {
        return VirtualValues.nodeValue(
                vertex.getId(),
                Values.stringArray(
                        Utils.getLabelName(vertex.getLabel(), Utils.getLabelTypes(dataType))),
                parseProperties(vertex.getPropertiesList(), dataType));
    }

    protected RelationshipValue parseEdge(IrResult.Edge edge, @Nullable RelDataType dataType) {
        return VirtualValues.relationshipValue(
                edge.getId(),
                VirtualValues.nodeValue(
                        edge.getSrcId(),
                        Values.stringArray(
                                Utils.getSrcLabelName(
                                        edge.getSrcLabel(), Utils.getLabelTypes(dataType))),
                        MapValue.EMPTY),
                VirtualValues.nodeValue(
                        edge.getDstId(),
                        Values.stringArray(
                                Utils.getDstLabelName(
                                        edge.getDstLabel(), Utils.getLabelTypes(dataType))),
                        MapValue.EMPTY),
                Values.stringValue(
                        Utils.getLabelName(edge.getLabel(), Utils.getLabelTypes(dataType))),
                parseProperties(edge.getPropertiesList(), dataType));
    }

    private MapValue parseProperties(List<IrResult.Property> properties, RelDataType dataType) {
        Map<String, AnyValue> valueMap = Maps.newLinkedHashMap();
        // data types of properties
        List<RelDataTypeField> typeFields = dataType.getFieldList();
        properties.forEach(
                k -> {
                    Common.NameOrId key = k.getKey();
                    // property key string which is used for display
                    String keyStr;
                    RelDataTypeField field;
                    switch (key.getItemCase()) {
                        case NAME:
                            // find target field in typeFields by property name
                            field =
                                    Utils.findFieldByPredicate(
                                            k1 -> k1.getName().equals(key.getName()), typeFields);
                            keyStr = key.getName();
                            break;
                        case ID:
                        default:
                            // find target field in typeFields by property id
                            field =
                                    Utils.findFieldByPredicate(
                                            k1 -> k1.getIndex() == key.getId(), typeFields);
                            keyStr =
                                    (field != null) ? field.getName() : String.valueOf(key.getId());
                    }
                    AnyValue value =
                            parseValue(k.getValue(), field != null ? field.getType() : null);
                    valueMap.put(keyStr, value);
                });
        return VirtualValues.fromMap(valueMap, valueMap.size(), 0);
    }

    protected AnyValue parseGraphPath(IrResult.GraphPath path, @Nullable RelDataType dataType) {
        List<NodeValue> nodes = Lists.newArrayList();
        List<RelationshipValue> relationships = Lists.newArrayList();
        path.getPathList()
                .forEach(
                        k -> {
                            switch (k.getInnerCase()) {
                                case VERTEX:
                                    nodes.add(
                                            parseVertex(
                                                    k.getVertex(), Utils.getVertexType(dataType)));
                                    break;
                                case EDGE:
                                    relationships.add(
                                            parseEdge(k.getEdge(), Utils.getEdgeType(dataType)));
                                    break;
                            }
                        });
        return VirtualValues.path(
                nodes.toArray(NodeValue[]::new), relationships.toArray(RelationshipValue[]::new));
    }

    protected AnyValue parseValue(Common.Value value, @Nullable RelDataType dataType) {
        if (dataType instanceof GraphLabelType) {
            return Values.stringValue(Utils.parseLabelValue(value, (GraphLabelType) dataType));
        }
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
            case DATE:
                Preconditions.checkArgument(
                        dataType.getSqlTypeName() == SqlTypeName.DATE,
                        "date32 value should have date type");
                return DateValue.epochDate(value.getDate().getItem());
            case TIME:
                Preconditions.checkArgument(
                        dataType.getSqlTypeName() == SqlTypeName.TIME,
                        "time32 value should have time type");
                return TimeValue.time(value.getTime().getItem() * 1000_000L, ZoneOffset.UTC);
            case TIMESTAMP:
                Preconditions.checkArgument(
                        dataType.getSqlTypeName() == SqlTypeName.TIMESTAMP,
                        "timestamp value should have timestamp type");
                return DateTimeValue.ofEpochMillis(
                        Values.longValue(value.getTimestamp().getItem()));
            default:
                throw new NotImplementedException(value.getItemCase() + " is unsupported yet");
        }
    }
}
