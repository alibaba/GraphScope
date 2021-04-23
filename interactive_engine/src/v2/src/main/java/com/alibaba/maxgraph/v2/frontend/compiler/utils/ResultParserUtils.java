package com.alibaba.maxgraph.v2.frontend.compiler.utils;

import com.alibaba.maxgraph.proto.v2.EntryProto;
import com.alibaba.maxgraph.proto.v2.ExtraEdgeEntityProto;
import com.alibaba.maxgraph.proto.v2.ExtraMessageProto;
import com.alibaba.maxgraph.proto.v2.ListBinary;
import com.alibaba.maxgraph.proto.v2.ListDouble;
import com.alibaba.maxgraph.proto.v2.ListFloat;
import com.alibaba.maxgraph.proto.v2.ListInt;
import com.alibaba.maxgraph.proto.v2.ListLong;
import com.alibaba.maxgraph.proto.v2.ListProto;
import com.alibaba.maxgraph.proto.v2.MapProto;
import com.alibaba.maxgraph.proto.v2.MessageType;
import com.alibaba.maxgraph.proto.v2.PathEntityProto;
import com.alibaba.maxgraph.proto.v2.PropertyEntityProto;
import com.alibaba.maxgraph.proto.v2.RawMessageProto;
import com.alibaba.maxgraph.proto.v2.ValueEntityProto;
import com.alibaba.maxgraph.proto.v2.ValuePropertyEntityProto;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.frontend.result.EdgeResult;
import com.alibaba.maxgraph.v2.common.frontend.result.EntryValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.QueryResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexPropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResultParserUtils {
    private static final Logger logger = LoggerFactory.getLogger(ResultParserUtils.class);

    private static String getVertexLabel(int labelId, long vertexId, GraphSchema schema) {
        try {
            return schema.getSchemaElement(labelId).getLabel();
        } catch (Exception e) {
            throw new RuntimeException("get label name for labelId[" + labelId + "] in vertex[" + vertexId + "] fail", e);
        }
    }

    private static String getEdgeLabel(int labelId, long edgeId, GraphSchema schema) {
        try {
            return schema.getSchemaElement(labelId).getLabel();
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

    private static VertexResult buildVertex(long id, int typeId, GraphSchema schema) {
        try {
            return new VertexResult(id, typeId, getVertexLabel(typeId, id, schema));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static VertexResult parseVertex(RawMessageProto message, GraphSchema schema) {
        long id = message.getId();
        int typeId = message.getTypeId();
        VertexResult v = buildVertex(id, typeId, schema);
        for (PropertyEntityProto p : message.getExtra().getExtraValueProp().getPropListList()) {
            Object value = parseValue(p.getPropValue());
            String name = getPropertyName(p.getPropId(), schema);
            v.addProperty(name, value);
        }
        return v;
    }

    private static EdgeResult parseEdge(RawMessageProto message, GraphSchema schema) {
        long id = message.getId();
        int typeId = message.getTypeId();

        ExtraEdgeEntityProto edgeEntity = message.getExtra().getExtraEdge();
        List<PropertyEntityProto> propertyList = message.getExtra().getExtraValueProp().getPropListList();

        String srcVertexLabel = getVertexLabel(edgeEntity.getSrcTypeId(), edgeEntity.getSrcId(), schema);
        String dstVertexLabel = getVertexLabel(edgeEntity.getDstTypeId(), edgeEntity.getDstId(), schema);
        EdgeResult edgeResult = new EdgeResult(edgeEntity.getSrcId(),
                edgeEntity.getSrcTypeId(),
                srcVertexLabel,
                edgeEntity.getDstId(),
                edgeEntity.getDstTypeId(),
                dstVertexLabel,
                new CompositeId(id, typeId),
                getEdgeLabel(typeId, id, schema));
        if (null != propertyList) {
            for (PropertyEntityProto propertyEntity : propertyList) {
                String name = getPropertyName(propertyEntity.getPropId(), schema);
                Object value = parseValue(propertyEntity.getPropValue());
                edgeResult.addProperty(name, value);
            }
        }

        return edgeResult;
    }

    private static Object parseValue(ValueEntityProto valueEntity) {
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
            case VT_INTEGER:
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
            case VT_INTEGER_LIST: {
                try {
                    ListInt listIntValue = ListInt.parseFrom(byteString);
                    return Lists.newArrayList(listIntValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_LONG_LIST: {
                try {
                    ListLong listLongValue = ListLong.parseFrom(byteString);
                    return Lists.newArrayList(listLongValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_FLOAT_LIST: {
                try {
                    ListFloat listFloatValue = ListFloat.parseFrom(byteString);
                    return Lists.newArrayList(listFloatValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_DOUBLE_LIST: {
                try {
                    ListDouble listDoubleValue = ListDouble.parseFrom(byteString);
                    return Lists.newArrayList(listDoubleValue.getValueList());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
            case VT_BYTES_LIST: {
                try {
                    ListBinary listBinaryValue = ListBinary.parseFrom(byteString);
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

    private static Object parseProperty(RawMessageProto message, GraphSchema schema) {
        String propName = getPropertyName((int) message.getId(), schema);
        Object propValue = parseValue(message.getExtra().getExtraValueProp().getValueEntity());
        if (message.getTypeId() == 0) {
            return new VertexPropertyResult<>(propName, propValue);
        } else {
            return new PropertyResult<>(propName, propValue);
        }
    }

    private static QueryResult parseEntry(RawMessageProto message, GraphSchema schema, Map<Integer, String> labelIdNameList) {
        try {
            EntryProto entry = EntryProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
            EntryValueResult entryValueResult = new EntryValueResult();
            entryValueResult.setKey(parseRawMessage(entry.getKey(), schema, labelIdNameList).get(0));
            entryValueResult.setValue(parseRawMessage(entry.getValue(), schema, labelIdNameList).get(0));
            return entryValueResult;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object parsePathEntry(RawMessageProto message, GraphSchema schema, Map<Integer, String> labelIdNameList) {
        try {
            PathEntityProto pathEntity = PathEntityProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
            Object pathValue = parseRawMessage(pathEntity.getMessage(), schema, labelIdNameList).get(0);
            List<Integer> labelIdList = pathEntity.getLabelListList();
            if (null != labelIdList && labelIdList.size() > 0) {
                Set<String> labelList = labelIdList.stream().map(labelIdNameList::get).collect(Collectors.toSet());
                return new PathValueResult(pathValue, labelList);
            } else {
                return new PathValueResult(pathValue);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Object> parseRawMessage(RawMessageProto message, GraphSchema schema, Map<Integer, String> labelIdNameList) {
        List<Object> queryResultList = Lists.newArrayList();
        long bulkValue = message.getBulk() > 0 ? message.getBulk() : 1;
        Object queryResult = null;
        switch (message.getMessageType()) {
            case MSG_VERTEX_TYPE: {
                queryResult = parseVertex(message, schema);
                break;
            }
            case MSG_EDGE_TYPE: {
                queryResult = parseEdge(message, schema);
                break;
            }
            case MSG_PROP_TYPE: {
                queryResult = parseProperty(message, schema);
                break;
            }
            case MSG_VALUE_TYPE: {
                queryResult = parseValue(message.getExtra().getExtraValueProp().getValueEntity());
                break;
            }
            case MSG_ENTRY_TYPE: {
                queryResult = parseEntry(message, schema, labelIdNameList);
                break;
            }
            case MSG_PATH_ENTRY_TYPE: {
                queryResult = parsePathEntry(message, schema, labelIdNameList);
                break;
            }
            case MSG_LIST_TYPE: {
                try {
                    ListProto listProto = ListProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
                    List list = Lists.newArrayList();
                    boolean pathFlag = true;
                    for (RawMessageProto listEntry : listProto.getValueList()) {
                        Object listValue = parseRawMessage(listEntry, schema, labelIdNameList).get(0);
                        pathFlag = pathFlag & listValue instanceof PathValueResult;
                        list.add(listValue);
                    }
                    if (pathFlag) {
                        queryResult = new PathResult((List<PathValueResult>) list);
                    } else {
                        queryResult = list;
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case MSG_MAP_TYPE: {
                try {
                    MapProto mapProto = MapProto.parseFrom(message.getExtra().getExtraValueProp().getValueEntity().getPayload());
                    queryResult = Maps.newHashMap();
                    for (EntryProto mapEntry : mapProto.getEntryListList()) {
                        ((Map) queryResult).put(parseRawMessage(mapEntry.getKey(), schema, labelIdNameList).get(0),
                                parseRawMessage(mapEntry.getValue(), schema, labelIdNameList).get(0));
                    }
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
        if (null != queryResult) {
            for (int i = 0; i < bulkValue; i++) {
                queryResultList.add(queryResult);
            }
        }
        return queryResultList;
    }

    public static RawMessageProto buildResponseMessage(Object result, GraphSchema schema) {
        RawMessageProto.Builder message = RawMessageProto.newBuilder();

        if (result instanceof VertexResult) {
            VertexResult vertex = (VertexResult) result;
            message.setMessageType(MessageType.MSG_VERTEX_TYPE)
                    .setId(vertex.getId())
                    .setTypeId(vertex.getLabelId());

            for (Map.Entry<String, Object> prop : vertex.getProperties().entrySet()) {
                int propId = SchemaUtils.getPropId(prop.getKey(), schema);
                message.getExtraBuilder().getExtraValuePropBuilder().addPropList(buildPropertyMessage(propId, prop.getValue()));
            }

        } else if (result instanceof EdgeResult) {
            EdgeResult edge = (EdgeResult) result;
            message.setMessageType(MessageType.MSG_EDGE_TYPE)
                    .setId(edge.getId().id())
                    .setTypeId(schema.getSchemaElement(edge.getLabel()).getLabelId());

            ExtraEdgeEntityProto extraEdgeEntity = ExtraEdgeEntityProto.newBuilder()
                    .setSrcId(edge.getSrcVertexId())
                    .setSrcTypeId(edge.getSrcLabelId())
                    .setDstId(edge.getDstVertexId())
                    .setDstTypeId(edge.getDstLabelId()).build();

            message.setExtra(ExtraMessageProto.newBuilder().setExtraEdge(extraEdgeEntity).build());

        } else if (result instanceof VertexPropertyResult) {
            logger.info("This is VertexPropertyResult");
            VertexPropertyResult propertyResult = (VertexPropertyResult) result;
            message.setMessageType(MessageType.MSG_PROP_TYPE)
                    .setId(SchemaUtils.getPropId(propertyResult.getName(), schema))
                    .setTypeId(0);

            message.getExtraBuilder().getExtraValuePropBuilder().setValueEntity(buildValueMessage(propertyResult.getValue()));

        } else if (result instanceof PropertyResult) {
            logger.info("This is PropertyResult");
            PropertyResult propertyResult = (PropertyResult) result;
            message.setMessageType(MessageType.MSG_PROP_TYPE)
                    .setId(SchemaUtils.getPropId(propertyResult.getName(), schema))
                    .setTypeId(1);//not 0

            message.getExtraBuilder().getExtraValuePropBuilder().setValueEntity(buildValueMessage(propertyResult.getValue()));

        } else if (result instanceof EntryValueResult) {
            logger.info("This is EntryValueResult");
        } else if (result instanceof PathValueResult) {
            logger.info("This is PathValueResult");
        } else if (result instanceof List) {
            logger.info("This is ListResult");
            List<Object> listResult = (List<Object>) result;

            message.setMessageType(MessageType.MSG_LIST_TYPE);
            ListProto.Builder listProto = ListProto.newBuilder();
            for (Object queryResult : listResult) {
                listProto.addValue(buildResponseMessage(queryResult, schema));
            }

            ValueEntityProto valueEntityProto = ValueEntityProto.newBuilder().setPayload(listProto.build().toByteString()).build();
            ValuePropertyEntityProto valuePropertyEntityProto = ValuePropertyEntityProto.newBuilder()
                    .setValueEntity(valueEntityProto).build();
            ExtraMessageProto extraMessageProto = ExtraMessageProto.newBuilder().setExtraValueProp(valuePropertyEntityProto).build();

            message.setExtra(extraMessageProto);

        } else if (result instanceof Map) {
            logger.info("This is MapValueResult");
        } else {
            logger.info("This is PropertyValueResult");
            message.setMessageType(MessageType.MSG_VALUE_TYPE)
                    .getExtraBuilder().getExtraValuePropBuilder().setValueEntity(buildValueMessage(result));
        }

        return message.build();
    }

    public static PropertyEntityProto buildPropertyMessage(int propId, Object propValue) {
        PropertyEntityProto.Builder prop = PropertyEntityProto.newBuilder()
                .setPropId(propId)
                .setPropValue(buildValueMessage(propValue));

        return prop.build();
    }

    public static ValueEntityProto buildValueMessage(Object value) {
        ValueEntityProto.Builder valueEntity = ValueEntityProto.newBuilder();
        if (value instanceof Boolean) {
            valueEntity.setValueType(VariantType.VT_BOOL);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Boolean) value)));
        } else if (value instanceof Character) {
            valueEntity.setValueType(VariantType.VT_CHAR);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Character) value)));
        } else if (value instanceof Short) {
            valueEntity.setValueType(VariantType.VT_SHORT);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Short) value)));
        } else if (value instanceof Integer) {
            valueEntity.setValueType(VariantType.VT_INTEGER);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Integer) value)));
        } else if (value instanceof Long) {
            valueEntity.setValueType(VariantType.VT_LONG);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Long) value)));
        } else if (value instanceof Float) {
            valueEntity.setValueType(VariantType.VT_FLOAT);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Float) value)));
        } else if (value instanceof Double) {
            valueEntity.setValueType(VariantType.VT_DOUBLE);
            valueEntity.setPayload(ByteString.copyFromUtf8(String.valueOf((Double) value)));
        } else if (value instanceof byte[]) {
            valueEntity.setValueType(VariantType.VT_BINARY);
            valueEntity.setPayload(ByteString.copyFrom((byte[]) value));
        } else if (value instanceof String) {
            valueEntity.setValueType(VariantType.VT_STRING);
            valueEntity.setPayload(ByteString.copyFromUtf8((String) value));
        } else if (value instanceof Date) {
            valueEntity.setValueType(VariantType.VT_DATE);
            Date date = (Date) value;
            valueEntity.setPayload(ByteString.copyFromUtf8(date.toString()));
        } else if (value instanceof List) {
            List listValue = (List) value;
            if (listValue.isEmpty() || listValue.get(0) instanceof Integer) {
                valueEntity.setValueType(VariantType.VT_INTEGER_LIST);
                ListInt listInt = ListInt.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listInt.toByteString());
            } else if (listValue.get(0) instanceof Long) {
                valueEntity.setValueType(VariantType.VT_LONG_LIST);
                ListLong listLong = ListLong.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listLong.toByteString());
            } else if (listValue.get(0) instanceof Float) {
                valueEntity.setValueType(VariantType.VT_FLOAT_LIST);
                ListFloat listFloat = ListFloat.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listFloat.toByteString());

            } else if (listValue.get(0) instanceof Double) {
                valueEntity.setValueType(VariantType.VT_DOUBLE_LIST);
                ListDouble listDouble = ListDouble.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listDouble.toByteString());
            } else if (listValue.get(0) instanceof Byte) {
                valueEntity.setValueType(VariantType.VT_BYTES_LIST);
                ListBinary listBinary = ListBinary.newBuilder().addAllValue(listValue).build();
                valueEntity.setPayload(listBinary.toByteString());
            } else {
                throw new UnsupportedOperationException("Prop Value type is not supported");
            }
        } else {
            throw new UnsupportedOperationException("Prop Value type is not supported");
        }

        return valueEntity.build();
    }

    public static List<Object> parseResponse(ByteString bytes, GraphSchema schema, Map<Integer, String> labelIdNameList) throws InvalidProtocolBufferException {
        RawMessageProto message = RawMessageProto.parseFrom(bytes);
        return parseRawMessage(message, schema, labelIdNameList);
    }
}
