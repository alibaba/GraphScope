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
package com.alibaba.maxgraph.compiler.utils;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.Message.PropertyEntityProto;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.result.EdgeResult;
import com.alibaba.maxgraph.result.ListResult;
import com.alibaba.maxgraph.result.MapValueResult;
import com.alibaba.maxgraph.result.PathValueResult;
import com.alibaba.maxgraph.result.PropertyResult;
import com.alibaba.maxgraph.result.PropertyValueResult;
import com.alibaba.maxgraph.result.VertexPropertyResult;
import com.alibaba.maxgraph.result.VertexResult;
import com.alibaba.maxgraph.result.BulkResult;
import com.alibaba.maxgraph.sdkcommon.graph.EntryValueResult;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class ResultParserUtils {
    private static final Logger logger = LoggerFactory.getLogger(ResultParserUtils.class);

    private static String getVertexLabel(int labelId, long vertexId, GraphSchema schema) {
        try {
            return schema.getElement(labelId).getLabel();
        } catch (Exception e) {
            throw new RuntimeException("get label name for labelId[" + labelId + "] in vertex[" + vertexId + "] fail", e);
        }
    }

    private static String getEdgeLabel(int labelId, long edgeId, GraphSchema schema) {
        try {
            return schema.getElement(labelId).getLabel();
        } catch (Exception e) {
            throw new RuntimeException("get label name for labelId[" + labelId + "] in edge[" + edgeId + "] fail", e);
        }
    }

    private static String getPropertyName(int propId, GraphSchema schema) {
        try {
            return SchemaUtils.getPropertyName(propId, schema);
        } catch (Exception e) {
            throw new RuntimeException("get property name for prop id[" + propId + "] fail", e);
        }
    }

    private static VertexResult parseVertex(long id, int typeId, GraphSchema schema, Graph graph, int storeId) {
        try {
            return new VertexResult(id, typeId, getVertexLabel(typeId, id, schema), graph, storeId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static VertexResult parseVertex(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        long id = message.getId();
        int typeId = message.getTypeId();
        int storeId = message.getStoreId();
        VertexResult v =  parseVertex(id, typeId, schema, graph, storeId);
        for (PropertyEntityProto p : message.getExtra().getExtraValueProp().getPropListList()) {
            Object value = parseValue(p.getPropValue(), schema, graph);
            String name = getPropertyName(p.getPropId(), schema);
            v.addProperty(new VertexPropertyResult(p.getPropId(), name, value, null));
        }
        return v;
    }

    private static EdgeResult parseEdge(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        long id = message.getId();
        int typeId = message.getTypeId();

        Message.ExtraEdgeEntityProto edgeEntity = message.getExtra().getExtraEdge();
        List<Message.PropertyEntityProto> propertyList = message.getExtra().getExtraValueProp().getPropListList();

        EdgeResult edgeResult = new EdgeResult(id,
                parseVertex(edgeEntity.getSrcId(), edgeEntity.getSrcTypeId(), schema, graph, 0),
                parseVertex(edgeEntity.getDstId(), edgeEntity.getDstTypeId(), schema, graph, 0),
                getEdgeLabel(typeId, id, schema));
        if (null != propertyList) {
            for (Message.PropertyEntityProto propertyEntity : propertyList) {
                edgeResult.addProperty(parseProperty(propertyEntity, edgeResult, schema, graph));
            }
        }

        return edgeResult;
    }

    private static PropertyResult parseProperty(Message.PropertyEntityProto propertyEntity, Element element, GraphSchema schema, Graph graph) {
        return new PropertyResult(getPropertyName(propertyEntity.getPropId(), schema), parseValue(propertyEntity.getPropValue(), schema, graph), element);
    }

    private static Object parseValue(Message.ValueEntityProto valueEntity, GraphSchema schema, Graph graph) {
        ByteString byteString = valueEntity.getPayload();
        ByteBuffer byteBuffer = byteString.asReadOnlyByteBuffer();
        switch (valueEntity.getValueType()) {
            case VT_BOOL: {
                try {
                    BoolValue boolValue = BoolValue.parseFrom(byteString);
                    return boolValue.getValue();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_CHAR: {
                return byteBuffer.getChar();
            }
            case VT_SHORT: {
                return byteBuffer.getShort();
            }
            case VT_INT:
                return byteBuffer.getInt();
            case VT_LONG:
                return byteBuffer.getLong();
            case VT_STRING:
            case VT_DATE:
                return new String(byteString.toByteArray());
            case VT_FLOAT:
                return byteBuffer.getFloat();
            case VT_DOUBLE:
                return byteBuffer.getDouble();
            case VT_BINARY:
                return byteString.toByteArray();
            case VT_INT_LIST: {
                try {
                    Message.ListInt listIntValue = Message.ListInt.parseFrom(byteString);
                    return Lists.newArrayList(listIntValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_LONG_LIST: {
                try {
                    Message.ListLong listLongValue = Message.ListLong.parseFrom(byteString);
                    return Lists.newArrayList(listLongValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_FLOAT_LIST: {
                try {
                    Message.ListFloat listFloatValue = Message.ListFloat.parseFrom(byteString);
                    return Lists.newArrayList(listFloatValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_DOUBLE_LIST: {
                try {
                    Message.ListDouble listDoubleValue = Message.ListDouble.parseFrom(byteString);
                    return Lists.newArrayList(listDoubleValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_BYTES_LIST: {
                try {
                    Message.ListBinary listBinaryValue = Message.ListBinary.parseFrom(byteString);
                    return Lists.newArrayList(listBinaryValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            default: {
                throw new UnsupportedOperationException(valueEntity.getValueType().toString());
            }
        }
    }

    private static QueryResult parseProperty(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        String propName = getPropertyName((int) message.getId(), schema);
        Object propValue = parseValue(message.getExtra().getExtraValueProp().getValueEntity(), schema, graph);
        if (message.getTypeId() == 0) {
            return new VertexPropertyResult<>((int) message.getId(), propName, propValue, new VertexResult(0, 0, "", graph, 0));
        } else {
            return new PropertyResult<>(propName, propValue, new VertexResult(0, 0, "", graph, 0));
        }
    }

    private static QueryResult parseEntry(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        try {
            Message.EntryProto entry = Message.EntryProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
            EntryValueResult entryValueResult = new EntryValueResult();
            entryValueResult.setKey(parseRawMessage(entry.getKey(), schema, graph));
            entryValueResult.setValue(parseRawMessage(entry.getValue(), schema, graph));
            return entryValueResult;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private static QueryResult parsePathEntry(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        try {
            Message.PathEntityProto pathEntity = Message.PathEntityProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
            QueryResult pathValue = parseRawMessage(pathEntity.getMessage(), schema, graph);
            PathValueResult pathValueResult = new PathValueResult(pathValue, Sets.newHashSet(pathEntity.getLabelListList()));
            return pathValueResult;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static QueryResult parseRawMessage(Message.RawMessageProto message, GraphSchema schema, Graph graph) {
        QueryResult queryResult = null;
        long bulkValue = message.getBulk() > 0 ? message.getBulk() : 1;
        switch (message.getMessageType()) {
            case MSG_VERTEX_TYPE: {
                queryResult = parseVertex(message, schema, graph);
                break;
            }
            case MSG_EDGE_TYPE: {
                queryResult = parseEdge(message, schema, graph);
                break;
            }
            case MSG_PROP_TYPE: {
                queryResult = parseProperty(message, schema, graph);
                break;
            }
            case MSG_VALUE_TYPE: {
                Object value = parseValue(message.getExtra().getExtraValueProp().getValueEntity(), schema, graph);
                if (value instanceof QueryResult) {
                    queryResult = (QueryResult)value;
                } else {
                    queryResult = new PropertyValueResult(value);
                }
                break;
            }
            case MSG_ENTRY_TYPE: {
                queryResult = parseEntry(message, schema, graph);
                break;
            }
            case MSG_PATH_ENTRY_TYPE: {
                queryResult = parsePathEntry(message, schema, graph);
                break;
            }
            case MSG_LIST_TYPE: {
                try {
                    Message.ListProto listProto = Message.ListProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
                    List<QueryResult> resultList = Lists.newArrayList();
                    for (Message.RawMessageProto listEntry : listProto.getValueList()) {
                        QueryResult result = parseRawMessage(listEntry, schema, graph);
                        if(result instanceof BulkResult) {
                            resultList.addAll(((BulkResult)result).getResultList());
                        }
                        else {
                            resultList.add(result);
                        }
                    }
                    queryResult = new ListResult(resultList);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case MSG_MAP_TYPE: {
                try {
                    Message.MapProto mapProto = Message.MapProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
                    MapValueResult mapList = new MapValueResult();
                    for (Message.EntryProto mapEntry : mapProto.getEntryListList()) {
                        mapList.addMapValue(parseRawMessage(mapEntry.getKey(), schema, graph), parseRawMessage(mapEntry.getValue(), schema, graph));
                    }
                    queryResult = mapList;
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case MSG_ERROR_TYPE: {
                String errorMessage = new String(message.getExtra().getExtraValueProp().getValueEntity().getPayload().toByteArray());
                throw new RuntimeException(errorMessage);
            }
            case MSG_DFS_CMD_TYPE:
            case UNRECOGNIZED: {
                throw new RuntimeException(message.getMessageType().toString());
            }
        }
        if (queryResult != null && bulkValue > 1) {
            return new BulkResult(Collections.nCopies((int)bulkValue, queryResult));
        }
        return queryResult;
    }

    public static Message.PropertyEntityProto buildPropertyMessage(VertexPropertyResult propertyResult){
        Message.PropertyEntityProto.Builder prop = Message.PropertyEntityProto.newBuilder();
        prop.setPropId((Integer) propertyResult.getPropId());

        Object value = propertyResult.value();
        prop.setPropValue(buildValueMessage(value));
        return prop.build();
    }

    public static Message.ValueEntityProto buildValueMessage(Object value){
        Message.ValueEntityProto.Builder valueEntity = Message.ValueEntityProto.newBuilder();
        if (value instanceof Boolean) {
            valueEntity.setValueType(Message.VariantType.VT_BOOL);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Boolean) value)));
        } else if (value instanceof Character) {
            valueEntity.setValueType(Message.VariantType.VT_CHAR);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Character) value)));
        } else if (value instanceof Short) {
            valueEntity.setValueType(Message.VariantType.VT_SHORT);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Short) value)));
        } else if (value instanceof Integer) {
            valueEntity.setValueType(Message.VariantType.VT_INT);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Integer) value)));
        } else if (value instanceof Long) {
            valueEntity.setValueType(Message.VariantType.VT_LONG);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Long) value)));
        } else if (value instanceof Float) {
            valueEntity.setValueType(Message.VariantType.VT_FLOAT);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Float) value)));
        } else if (value instanceof Double) {
            valueEntity.setValueType(Message.VariantType.VT_DOUBLE);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Double) value)));
        } else if (value instanceof byte[]) {
            valueEntity.setValueType(Message.VariantType.VT_BINARY);
            valueEntity.setPayload(ByteString.copyFrom((byte[]) value));
        } else if (value instanceof String) {
            valueEntity.setValueType(Message.VariantType.VT_STRING);
            valueEntity.setPayload(ByteString.copyFromUtf8((String) value));
        } else if (value instanceof Date) {
            valueEntity.setValueType(Message.VariantType.VT_DATE);
            Date date = (Date) value;
            valueEntity.setPayload(ByteString.copyFromUtf8(date.toString()));
        } else if (value instanceof List) {
            List listValue = (List) value;
            if (listValue.isEmpty() || listValue.get(0) instanceof Integer) {
                valueEntity.setValueType(Message.VariantType.VT_INT_LIST);
                Message.ListInt listInt = Message.ListInt.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listInt.toByteString());
            } else if (listValue.get(0) instanceof Long) {
                valueEntity.setValueType(Message.VariantType.VT_LONG_LIST);
                Message.ListLong listLong = Message.ListLong.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listLong.toByteString());
            } else if (listValue.get(0) instanceof Float) {
                valueEntity.setValueType(Message.VariantType.VT_FLOAT_LIST);
                Message.ListFloat listFloat = Message.ListFloat.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listFloat.toByteString());

            } else if (listValue.get(0) instanceof Double) {
                valueEntity.setValueType(Message.VariantType.VT_DOUBLE_LIST);
                Message.ListDouble listDouble = Message.ListDouble.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listDouble.toByteString());
            } else if (listValue.get(0) instanceof Byte) {
                valueEntity.setValueType(Message.VariantType.VT_BYTES_LIST);
                Message.ListBinary listBinary = Message.ListBinary.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listBinary.toByteString());
            } else {
                throw new UnsupportedOperationException("Prop Value type is not supported");
            }
        } else {
            throw new UnsupportedOperationException("Prop Value type is not supported");
        }

        return valueEntity.build();
    }

    public static QueryResult parseResponse(ByteString bytes, GraphSchema schema, Graph graph) throws InvalidProtocolBufferException {
        Message.RawMessageProto message = Message.RawMessageProto.parseFrom(bytes);
        return parseRawMessage(message, schema, graph);
    }
}
