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

package com.alibaba.graphscope.gremlin.result.parser;

import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPxdElementType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.result.ResultParser;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;
import com.alibaba.graphscope.gremlin.result.EmptyValue;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CypherResultParser implements ResultParser {
    private static final Logger logger = LoggerFactory.getLogger(CypherResultParser.class);
    private RelDataType outputType;

    public CypherResultParser(RelDataType outputType) {
        this.outputType = Objects.requireNonNull(outputType);
    }

    @Override
    public List<Object> parseFrom(PegasusClient.JobResponse response) {
        try {
            IrResult.Results results = IrResult.Results.parseFrom(response.getResp());
            Map<String, Object> columns = new LinkedHashMap<>();
            outputType
                    .getFieldList()
                    .forEach(
                            k -> {
                                IrResult.Column column =
                                        getIrColumn(k, results.getRecord().getColumnsList());
                                Object entryValue = parseEntry(column.getEntry(), k.getType());
                                columns.put(
                                        k.getName(),
                                        (entryValue == EmptyValue.INSTANCE) ? null : entryValue);
                            });
            return ImmutableList.of(columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Vertex parseVertex(IrResult.Vertex vertex, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType,
                "data type of vertex should be " + GraphSchemaType.class);
        return new DetachedVertex(
                vertex.getId(),
                getLabelName(vertex.getLabel(), getLabelTypes((GraphSchemaType) dataType)),
                ImmutableMap.of());
    }

    protected Edge parseEdge(IrResult.Edge edge, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(
                dataType instanceof GraphSchemaType,
                "data type of edge should be " + GraphSchemaType.class);
        List<GraphLabelType> labelTypes = getLabelTypes((GraphSchemaType) dataType);
        return new DetachedEdge(
                edge.getId(),
                getLabelName(edge.getLabel(), labelTypes),
                ImmutableMap.of(),
                edge.getSrcId(),
                getSrcLabelName(edge.getSrcLabel(), labelTypes),
                edge.getDstId(),
                getDstLabelName(edge.getDstLabel(), labelTypes));
    }

    protected List<Element> parseGraphPath(
            IrResult.GraphPath path, @Nullable RelDataType dataType) {
        Preconditions.checkArgument(dataType.getSqlTypeName() == SqlTypeName.ARRAY);
        ArraySqlType arrayType = (ArraySqlType) dataType;
        Preconditions.checkArgument(arrayType.getComponentType() instanceof GraphPxdElementType);
        GraphPxdElementType elementType = (GraphPxdElementType) arrayType.getComponentType();
        return path.getPathList().stream()
                .map(
                        k -> {
                            switch (k.getInnerCase()) {
                                case VERTEX:
                                    return parseVertex(k.getVertex(), elementType.getGetVType());
                                case EDGE:
                                default:
                                    return parseEdge(k.getEdge(), elementType.getExpandType());
                            }
                        })
                .collect(Collectors.toList());
    }

    protected Object parseValue(Common.Value value, @Nullable RelDataType dataType) {
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
            case PAIR_ARRAY:
                Common.PairArray pairs = value.getPairArray();
                Map pairInMap = new HashMap();
                // todo: update the dataType of key and value in a pair
                pairs.getItemList()
                        .forEach(
                                pair -> {
                                    pairInMap.put(
                                            parseValue(pair.getKey(), dataType),
                                            parseValue(pair.getVal(), dataType));
                                });
                return pairInMap;
            case STR_ARRAY:
                return value.getStrArray().getItemList();
            case NONE:
                return EmptyValue.INSTANCE;
            default:
                throw new GremlinResultParserException(value.getItemCase() + " is unsupported yet");
        }
    }

    protected List<Object> parseCollection(
            IrResult.Collection collection, @Nullable RelDataType dataType) {
        // multiset is the data type of aggregate function collect
        // array is the data type of path collection
        Preconditions.checkArgument(
                dataType.getSqlTypeName() == SqlTypeName.MULTISET
                        || dataType.getSqlTypeName() == SqlTypeName.ARRAY);
        return collection.getCollectionList().stream()
                .map(k -> parseElement(k, dataType.getComponentType()))
                .collect(Collectors.toList());
    }

    protected Object parseEntry(IrResult.Entry entry, @Nullable RelDataType dataType) {
        switch (entry.getInnerCase()) {
            case ELEMENT:
                return parseElement(entry.getElement(), dataType);
            case COLLECTION:
            default:
                List<Object> elements = parseCollection(entry.getCollection(), dataType);
                List notNull =
                        elements.stream()
                                .filter(k -> !(k instanceof EmptyValue))
                                .collect(Collectors.toList());
                return notNull.isEmpty() ? EmptyValue.INSTANCE : notNull;
        }
    }

    protected Object parseElement(IrResult.Element element, @Nullable RelDataType dataType) {
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

    private IrResult.Column getIrColumn(RelDataTypeField field, List<IrResult.Column> columnList) {
        List<Object> existColumns = new ArrayList<>();
        for (IrResult.Column column : columnList) {
            Common.NameOrId nameOrId = column.getNameOrId();
            switch (nameOrId.getItemCase()) {
                case ID:
                    if (field.getIndex() == nameOrId.getId()) {
                        return column;
                    }
                    existColumns.add(nameOrId.getId());
                    break;
                case NAME:
                default:
                    // hack: column id is in string format
                    if (nameOrId.getName().matches("^[0-9]+$")) {
                        int nameId = Integer.valueOf(nameOrId.getName());
                        if (field.getIndex() == nameId) {
                            return column;
                        }
                        existColumns.add(nameId);
                    } else {
                        if (field.getName().equals(nameOrId.getName())) {
                            return column;
                        }
                        existColumns.add(nameOrId.getName());
                    }
            }
        }
        throw new IllegalArgumentException(
                "field " + field + " not found, expected columns are" + existColumns);
    }
}
