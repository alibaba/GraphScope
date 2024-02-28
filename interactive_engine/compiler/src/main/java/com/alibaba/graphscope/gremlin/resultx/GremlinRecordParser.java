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

package com.alibaba.graphscope.gremlin.resultx;

import com.alibaba.graphscope.common.ir.type.ArbitraryArrayType;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.result.RecordParser;
import com.alibaba.graphscope.common.result.Utils;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GremlinRecordParser implements RecordParser<Object> {
    protected final ResultSchema resultSchema;

    public GremlinRecordParser(ResultSchema resultSchema) {
        this.resultSchema = resultSchema;
    }

    @Override
    public List<Object> parseFrom(IrResult.Record record) {
        RelDataType outputType = resultSchema.outputType;
        Preconditions.checkArgument(
                record.getColumnsCount() == outputType.getFieldCount(),
                "column size of results "
                        + record.getColumnsCount()
                        + " should be consistent with output type "
                        + outputType.getFieldCount());
        Map<Object, Object> mapValue = Maps.newLinkedHashMap();
        if (resultSchema.isGroupBy) {
            Preconditions.checkArgument(
                    resultSchema.groupKeyCount <= outputType.getFieldCount(),
                    "invalid group key count " + resultSchema.groupKeyCount);
            List<Object> groupKeys = Lists.newArrayList();
            for (int i = 0; i < resultSchema.groupKeyCount; i++) {
                IrResult.Column column = record.getColumns(i);
                RelDataTypeField field = outputType.getFieldList().get(i);
                groupKeys.add(parseEntry(column.getEntry(), field.getType()));
            }
            List<Object> groupValues = Lists.newArrayList();
            for (int i = resultSchema.groupKeyCount; i < record.getColumnsCount(); i++) {
                IrResult.Column column = record.getColumns(i);
                RelDataTypeField field = outputType.getFieldList().get(i);
                Object value = parseEntry(column.getEntry(), field.getType());
                if (value != null) {
                    groupValues.add(value);
                }
            }
            if (!groupValues.isEmpty() && !groupKeys.isEmpty()) {
                mapValue.put(
                        groupKeys.size() == 1 ? groupKeys.get(0) : groupKeys,
                        groupValues.size() == 1 ? groupValues.get(0) : groupValues);
            }
        } else {
            for (int i = 0; i < record.getColumnsCount(); i++) {
                IrResult.Column column = record.getColumns(i);
                RelDataTypeField field = outputType.getFieldList().get(i);
                mapValue.put(field.getName(), parseEntry(column.getEntry(), field.getType()));
            }
        }
        return Lists.newArrayList(
                !resultSchema.isGroupBy && mapValue.size() == 1
                        ? mapValue.values().iterator().next()
                        : mapValue);
    }

    @Override
    public RelDataType schema() {
        return this.resultSchema.outputType;
    }

    private Object parseEntry(IrResult.Entry entry, RelDataType type) {
        if (type instanceof GraphPathType) {
            return parseElement(entry.getElement(), type);
        }
        switch (type.getSqlTypeName()) {
            case MULTISET:
            case ARRAY:
                if (type instanceof ArbitraryArrayType) {
                    return parseCollection(
                            entry.getCollection(), ((ArbitraryArrayType) type).getComponentTypes());
                } else {
                    return parseCollection(entry.getCollection(), type.getComponentType());
                }
            case MAP:
                if (type instanceof ArbitraryMapType) {
                    return parseKeyValues(
                            entry.getMap(),
                            ((ArbitraryMapType) type).getKeyTypes(),
                            ((ArbitraryMapType) type).getValueTypes());
                } else {
                    return parseKeyValues(entry.getMap(), type.getKeyType(), type.getValueType());
                }
            default:
                return parseElement(entry.getElement(), type);
        }
    }

    private List<Object> parseCollection(
            IrResult.Collection collection, RelDataType componentType) {
        return collection.getCollectionList().stream()
                .map(k -> parseElement(k, componentType))
                .collect(Collectors.toList());
    }

    private List<Object> parseCollection(
            IrResult.Collection collection, List<RelDataType> componentTypes) {
        List<IrResult.Element> elements = collection.getCollectionList();
        Preconditions.checkArgument(
                elements.size() == componentTypes.size(),
                "Collection element size="
                        + elements.size()
                        + " is not consistent with type size="
                        + componentTypes.size());
        List<Object> values = Lists.newArrayList();
        for (int i = 0; i < elements.size(); ++i) {
            values.add(parseElement(elements.get(i), componentTypes.get(i)));
        }
        return values;
    }

    private Map<Object, Object> parseKeyValues(
            IrResult.KeyValues keyValues, RelDataType keyType, RelDataType valueType) {
        Map<Object, Object> valueMap = Maps.newLinkedHashMap();
        keyValues
                .getKeyValuesList()
                .forEach(
                        entry -> {
                            Object value = parseElement(entry.getValue(), valueType);
                            if (value != null) {
                                valueMap.put(parseMapKey(entry.getKey().getStr()), value);
                            }
                        });
        return valueMap;
    }

    private Map<Object, Object> parseKeyValues(
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
        Map<Object, Object> valueMap = Maps.newLinkedHashMap();
        for (int i = 0; i < entries.size(); ++i) {
            IrResult.KeyValues.KeyValue entry = entries.get(i);
            Object value = parseElement(entry.getValue(), valueTypes.get(i));
            if (value != null) {
                valueMap.put(parseMapKey(entry.getKey().getStr()), value);
            }
        }
        return valueMap;
    }

    private Object parseMapKey(String mapKey) {
        if (mapKey.equals(T.id.getAccessor())) {
            return T.id;
        } else if (mapKey.equals(T.label.getAccessor())) {
            return T.label;
        } else if (mapKey.equals(T.key.getAccessor())) {
            return T.key;
        } else if (mapKey.equals(T.value.getAccessor())) {
            return T.value;
        } else {
            return mapKey;
        }
    }

    private Object parseElement(IrResult.Element element, RelDataType type) {
        switch (element.getInnerCase()) {
            case VERTEX:
                return parseVertex(element.getVertex(), type);
            case EDGE:
                return parseEdge(element.getEdge(), type);
            case GRAPH_PATH:
                return parseGraphPath(element.getGraphPath(), type);
            case OBJECT:
            default:
                return parseValue(element.getObject(), type);
        }
    }

    private Vertex parseVertex(IrResult.Vertex vertex, RelDataType type) {
        return new DetachedVertex(
                vertex.getId(),
                Utils.getLabelName(vertex.getLabel(), Utils.getLabelTypes(type)),
                parseVertexProperties(vertex, type));
    }

    private Edge parseEdge(IrResult.Edge edge, RelDataType type) {
        GraphLabelType labelType = Utils.getLabelTypes(type);
        return new DetachedEdge(
                edge.getId(),
                Utils.getLabelName(edge.getLabel(), labelType),
                parseEdgeProperties(edge, type),
                edge.getSrcId(),
                Utils.getSrcLabelName(edge.getSrcLabel(), labelType),
                edge.getDstId(),
                Utils.getDstLabelName(edge.getDstLabel(), labelType));
    }

    protected Map<String, Object> parseVertexProperties(IrResult.Vertex vertex, RelDataType type) {
        return ImmutableMap.of();
    }

    protected Map<String, Object> parseEdgeProperties(IrResult.Edge edge, RelDataType type) {
        return ImmutableMap.of();
    }

    private List<Element> parseGraphPath(IrResult.GraphPath path, @Nullable RelDataType dataType) {
        return path.getPathList().stream()
                .map(
                        k -> {
                            switch (k.getInnerCase()) {
                                case VERTEX:
                                    return parseVertex(
                                            k.getVertex(), Utils.getVertexType(dataType));
                                case EDGE:
                                    return parseEdge(k.getEdge(), Utils.getEdgeType(dataType));
                                default:
                                    throw new GremlinResultParserException(
                                            k.getInnerCase() + " is invalid");
                            }
                        })
                .collect(Collectors.toList());
    }

    protected @Nullable Object parseValue(Common.Value value, @Nullable RelDataType dataType) {
        if (dataType instanceof GraphLabelType) {
            return Utils.parseLabelValue(value, (GraphLabelType) dataType);
        }
        switch (value.getItemCase()) {
            case BOOLEAN:
                return value.getBoolean();
            case I32:
                return value.getI32();
            case I64:
                return value.getI64();
            case F64:
                return value.getF64();
            case STR:
                return value.getStr();
            case I32_ARRAY:
                return value.getI32Array().getItemList();
            case I64_ARRAY:
                return value.getI64Array().getItemList();
            case F64_ARRAY:
                return value.getF64Array().getItemList();
            case STR_ARRAY:
                return value.getStrArray().getItemList();
            case NONE:
                return null;
                // todo: support temporal types
            default:
                throw new NotImplementedException(value.getItemCase() + " is unsupported yet");
        }
    }
}
