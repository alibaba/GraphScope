package com.alibaba.graphscope.groot.dataload.databuild;

import com.alibaba.graphscope.groot.common.exception.PropertyDefNotFoundException;
import com.alibaba.graphscope.groot.common.schema.api.*;
import com.alibaba.graphscope.groot.common.schema.wrapper.PropertyValue;
import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.proto.groot.DataLoadTargetPb;
import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {
    // For Main Job
    static List<DataLoadTargetPb> getDataLoadTargets(
            Map<String, FileColumnMapping> columnMappingConfig) {
        List<DataLoadTargetPb> targets = new ArrayList<>();
        for (FileColumnMapping fileColumnMapping : columnMappingConfig.values()) {
            DataLoadTargetPb.Builder builder = DataLoadTargetPb.newBuilder();
            builder.setLabel(fileColumnMapping.getLabel());
            if (fileColumnMapping.getSrcLabel() != null) {
                builder.setSrcLabel(fileColumnMapping.getSrcLabel());
            }
            if (fileColumnMapping.getDstLabel() != null) {
                builder.setDstLabel(fileColumnMapping.getDstLabel());
            }
            targets.add(builder.build());
        }
        return targets;
    }

    public static Map<String, FileColumnMapping> parseColumnMapping(String str)
            throws JsonProcessingException {
        ObjectMapper m = new ObjectMapper();
        return m.readValue(str, new TypeReference<Map<String, FileColumnMapping>>() {});
    }

    public static Map<String, ColumnMappingInfo> getMappingInfo(
            Odps odps, GraphSchema schema, Map<String, FileColumnMapping> config) {
        Map<String, ColumnMappingInfo> columnMappingInfo = new HashMap<>();
        config.forEach(
                (fileName, fileColumnMapping) -> {
                    ColumnMappingInfo subInfo = fileColumnMapping.toColumnMappingInfo(schema);
                    // Note the project and partition is stripped (if exists)
                    columnMappingInfo.put(Utils.getTableIdentifier(odps, fileName), subInfo);
                });
        return columnMappingInfo;
    }

    public static String getTableIdentifier(Odps odps, String tableFullName) {
        TableInfo info = parseTableURL(odps, tableFullName);
        return info.getTableName() + "|" + info.getPartPath();
    }

    /**
     * Parse table URL to @TableInfo
     * @param url the pattern of [projectName.]tableName[|partitionSpec]
     * @return TableInfo
     */
    public static TableInfo parseTableURL(Odps odps, String url) {
        String projectName = odps.getDefaultProject();
        String tableName;
        String partitionSpec = null;
        if (url.contains(".")) {
            String[] items = url.split("\\.");
            projectName = items[0];
            tableName = items[1];
        } else {
            tableName = url;
        }
        if (tableName.contains("|")) {
            String[] items = tableName.split("\\|");
            tableName = items[0];
            partitionSpec = items[1];
        }

        TableInfo.TableInfoBuilder builder = TableInfo.builder();
        builder.projectName(projectName);

        builder.tableName(tableName);
        if (partitionSpec != null) {
            builder.partSpec(partitionSpec);
        }
        return builder.build();
    }

    public static Map<Long, DataLoadTargetPb> getTableToTargets(
            GraphSchema schema, Map<String, ColumnMappingInfo> info) {
        Map<Long, DataLoadTargetPb> tableToTarget = new HashMap<>();
        for (ColumnMappingInfo columnMappingInfo : info.values()) {
            long tableId = columnMappingInfo.getTableId();
            int labelId = columnMappingInfo.getLabelId();
            GraphElement graphElement = schema.getElement(labelId);
            String label = graphElement.getLabel();
            DataLoadTargetPb.Builder builder = DataLoadTargetPb.newBuilder();
            builder.setLabel(label);
            if (graphElement instanceof GraphEdge) {
                builder.setSrcLabel(
                        schema.getElement(columnMappingInfo.getSrcLabelId()).getLabel());
                builder.setDstLabel(
                        schema.getElement(columnMappingInfo.getDstLabelId()).getLabel());
            }
            tableToTarget.put(tableId, builder.build());
        }
        return tableToTarget;
    }

    public static GrootClient getClient(String endpoint, String username, String password) {
        return GrootClient.newBuilder()
                .setHosts(endpoint)
                .setUsername(username)
                .setPassword(password)
                .build();
    }

    public static boolean parseBoolean(String str) {
        return str.equalsIgnoreCase("true");
    }

    public static String replaceVars(String str, String[] args) {
        // User could specify a list of `key=value` pairs in command line,
        // to substitute variables like `${key}` in configStr
        for (String arg : args) {
            String[] kv = arg.split("=");
            if (kv.length == 2) {
                String key = "${" + kv[0] + "}";
                str = str.replace(key, kv[1]);
            }
        }
        return str;
    }

    /// For Mapper
    public static Map<Integer, PropertyValue> buildProperties(
            GraphElement typeDef, String[] items, Map<Integer, Integer> mapping) {
        Map<Integer, PropertyValue> operationProperties = new HashMap<>(mapping.size());
        mapping.forEach(
                (colIdx, propertyId) -> {
                    GraphProperty prop = typeDef.getProperty(propertyId);
                    String label = typeDef.getLabel();
                    String msg = "Label [" + label + "]: ";
                    if (prop == null) {
                        msg += "propertyId [" + propertyId + "] not found";
                        throw new PropertyDefNotFoundException(msg);
                    }
                    if (colIdx >= items.length) {
                        msg += "Invalid mapping [" + colIdx + "]->[" + propertyId + "]";
                        msg += "Data: " + Arrays.toString(items);
                        throw new IllegalArgumentException(msg);
                    }
                    PropertyValue propertyValue = null;
                    if (items[colIdx] != null) {
                        propertyValue = new PropertyValue(prop.getDataType(), items[colIdx]);
                    }
                    operationProperties.put(propertyId, propertyValue);
                });
        return operationProperties;
    }

    public static BytesRef getVertexKeyRef(
            DataEncoder encoder,
            GraphVertex type,
            Map<Integer, PropertyValue> properties,
            long tableId) {
        return encoder.encodeVertexKey(type, properties, tableId);
    }

    public static BytesRef getEdgeKeyRef(
            DataEncoder encoder,
            GraphSchema schema,
            ColumnMappingInfo info,
            String[] items,
            Map<Integer, PropertyValue> properties,
            long tableId,
            boolean forward) {
        int labelId = info.getLabelId();
        GraphEdge type = (GraphEdge) schema.getElement(labelId);

        int srcLabelId = info.getSrcLabelId();
        Map<Integer, Integer> srcPkColMap = info.getSrcPkColMap();
        GraphVertex srcType = (GraphVertex) schema.getElement(srcLabelId);
        Map<Integer, PropertyValue> srcPkMap = Utils.buildProperties(srcType, items, srcPkColMap);

        int dstLabelId = info.getDstLabelId();
        Map<Integer, Integer> dstPkColMap = info.getDstPkColMap();
        GraphVertex dstType = (GraphVertex) schema.getElement(dstLabelId);
        Map<Integer, PropertyValue> dstPkMap = Utils.buildProperties(dstType, items, dstPkColMap);

        return encoder.encodeEdgeKey(
                srcType, srcPkMap, dstType, dstPkMap, type, properties, tableId, forward);
    }

    public static String[] parseRecords(Record record) {
        String[] items = new String[record.getColumnCount()];
        for (int i = 0; i < items.length; i++) {
            Object curRecord = record.get(i);
            items[i] = curRecord == null ? null : curRecord.toString();
        }
        return items;
    }

    public static String convertDateForLDBC(String input) {
        SimpleDateFormat SRC_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        SimpleDateFormat DST_FMT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        DST_FMT.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
        try {
            return DST_FMT.format(SRC_FMT.parse(input));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
