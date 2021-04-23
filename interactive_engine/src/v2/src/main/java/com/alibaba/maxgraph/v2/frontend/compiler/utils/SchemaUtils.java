package com.alibaba.maxgraph.v2.frontend.compiler.utils;

import com.alibaba.maxgraph.proto.v2.RuntimeLabelIdNameProto;
import com.alibaba.maxgraph.proto.v2.RuntimePropertyIdNameProto;
import com.alibaba.maxgraph.proto.v2.RuntimeSchemaProto;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.*;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaUtils {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);
    private static final Set<String> tokenNameList = Sets.newHashSet(T.label.getAccessor(), T.id.getAccessor(), T.key.getAccessor(), T.value.getAccessor());

    /**
     * Get primary key list for the given vertex type
     *
     * @param vertexType The given vertex type
     * @return The primary key id list
     */
    public static List<Integer> getVertexPrimaryKeyList(VertexType vertexType) {
        return vertexType.getPrimaryKeyConstraint().getPrimaryKeyList().stream().map(v -> vertexType.getProperty(v).getId()).collect(Collectors.toList());
    }

    public static List<Integer> getEdgePrimaryKeyList(EdgeType edgeType) {
        PrimaryKeyConstraint primaryKeyConstraint = edgeType.getPrimaryKeyConstraint();
        if (primaryKeyConstraint == null) {
            return null;
        }
        return primaryKeyConstraint.getPrimaryKeyList().stream()
                .map(v -> edgeType.getProperty(v).getId()).collect(Collectors.toList());
    }

    /**
     * Check if the given property id is primary key in the given vertex type
     *
     * @param propId     The given property id
     * @param schema     The given schema
     * @param vertexType The given vertex type
     * @return The true means the property id is primary key
     */
    public static boolean isPropPrimaryKey(int propId, GraphSchema schema, VertexType vertexType) {
        String propName = getPropertyName(propId, schema);
        return vertexType.getPrimaryKeyConstraint().getPrimaryKeyList().contains(propName);
    }

    /**
     * Check if the given property name exist in the schema
     *
     * @param propName The given property name
     * @param schema   The given property
     * @return The true means the property name exist in the schema
     */
    public static boolean checkPropExist(String propName, GraphSchema schema) {
        try {
            Map<Integer, Integer> labelPropertyIds = schema.getPropertyId(propName);
            return (null != labelPropertyIds) && (!labelPropertyIds.isEmpty());
        } catch (Exception ignored) {
            return false;
        }
    }

    /**
     * Get property name with the given property id and schema
     *
     * @param propId The given property id
     * @param schema The given schema
     * @return The property name
     */
    public static String getPropertyName(int propId, GraphSchema schema) {
        Map<String, String> labelPropertyNames = schema.getPropertyName(propId);
        Set<String> propertyNames = Sets.newHashSet(labelPropertyNames.values());
        if (propertyNames.size() > 1) {
            throw new UnsupportedOperationException("there're multiple property name for property id " + propId + " in " + labelPropertyNames);
        }
        return propertyNames.iterator().next();
    }

    /**
     * Get property id with the given property name and schema
     *
     * @param name   The given property name
     * @param schema The given schema
     * @return The property name
     */
    public static int getPropId(String name, GraphSchema schema) {
        Map<Integer, Integer> labelPropertyIds = schema.getPropertyId(name);
        Set<Integer> propertyIds = Sets.newHashSet(labelPropertyIds.values());
        if (propertyIds.size() > 1) {
            throw new UnsupportedOperationException("there're multiple property id for property name " + name + " in " + labelPropertyIds);
        }
        return propertyIds.isEmpty() ? TreeConstants.MAGIC_PROP_ID : propertyIds.iterator().next();
    }

    /**
     * Get data type list for given property name and schema
     *
     * @param propName The given property name
     * @param schema   The given schema
     * @return The given property data type list
     */
    public static Set<DataType> getDataTypeList(String propName, GraphSchema schema) {
        Set<DataType> dataTypeList = Sets.newHashSet();
        if (tokenNameList.contains(propName)) {
            return dataTypeList;
        }

        for (GraphProperty propertyDef : schema.getPropertyDefinitions(propName).values()) {
            dataTypeList.add(propertyDef.getDataType());
        }

        return dataTypeList;
    }

    /**
     * Build runtime schema proto for query plan
     *
     * @param schema The given schema
     * @return The runtime schema proto
     */
    public static RuntimeSchemaProto buildRuntimeSchema(GraphSchema schema) {
        RuntimeSchemaProto.Builder schemaBuilder = RuntimeSchemaProto.newBuilder();
        List<RuntimeLabelIdNameProto> labelIdNameProtoList = Lists.newArrayList();
        Set<Integer> propertyIdSet = Sets.newHashSet();
        for (VertexType vertexType : schema.getVertexTypes()) {
            labelIdNameProtoList.add(RuntimeLabelIdNameProto.newBuilder()
                    .setLabelId(vertexType.getLabelId())
                    .setLabelName(vertexType.getLabel())
                    .build());
            propertyIdSet.addAll(vertexType.getPropertyList().stream().map(GraphProperty::getId).collect(Collectors.toSet()));
        }
        for (EdgeType edgeType : schema.getEdgeTypes()) {
            labelIdNameProtoList.add(RuntimeLabelIdNameProto.newBuilder()
                    .setLabelId(edgeType.getLabelId())
                    .setLabelName(edgeType.getLabel())
                    .build());
            propertyIdSet.addAll(edgeType.getPropertyList().stream().map(GraphProperty::getId).collect(Collectors.toSet()));
        }


        List<RuntimePropertyIdNameProto> propertyIdNameProtoList = Lists.newArrayList();
        for (Integer propertyId : propertyIdSet) {
            propertyIdNameProtoList.add(RuntimePropertyIdNameProto.newBuilder()
                    .setPropertyId(propertyId)
                    .setPropertyName(getPropertyName(propertyId, schema))
                    .build());
        }

        return schemaBuilder.addAllLabels(labelIdNameProtoList)
                .addAllProperties(propertyIdNameProtoList)
                .build();
    }
}
