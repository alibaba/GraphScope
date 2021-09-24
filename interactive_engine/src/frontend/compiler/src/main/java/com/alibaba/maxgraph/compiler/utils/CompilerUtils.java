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
import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.compiler.tree.*;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.*;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimMatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimPredicate;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CompilerUtils {
    public static long parseLongIdValue(Object id, boolean vertexFlag) {
        if (id instanceof CompositeId) {
            return ((CompositeId) id).id();
        } else if (id instanceof Vertex) {
            return CompositeId.class.cast(Vertex.class.cast(id).id()).id();
        } else if (id instanceof Edge) {
            return (long) ((Edge) id).id();
        } else {
            String idStrValue = id.toString();
            if (vertexFlag && StringUtils.startsWith(idStrValue, "v[")) {
                idStrValue = StringUtils.removeEnd(StringUtils.removeStart(idStrValue, "v["), "]");
            } else if (!vertexFlag && StringUtils.startsWith(idStrValue, "e[")) {
                String edgeIdStr = StringUtils.split(idStrValue, "]")[0];
                idStrValue = StringUtils.removeEnd(StringUtils.removeStart(edgeIdStr, "e["), "]");
            }
            if (StringUtils.contains(idStrValue, ".")) {
                return Long.parseLong(StringUtils.substring(idStrValue, idStrValue.lastIndexOf(".") + 1));
            } else {
                return Long.parseLong(idStrValue);
            }
        }
    }

    public static Message.LogicalCompare parseLogicalCompare(HasContainer hasContainer, GraphSchema schema, Map<String, Integer> labelIndexList, boolean vertexFlag) {
        Message.LogicalCompare.Builder logicalCompareBuilder = Message.LogicalCompare.newBuilder();
        String key = hasContainer.getKey();
        if (StringUtils.isEmpty(key)) {
            logicalCompareBuilder.setPropId(0);
        } else {
            if (SchemaUtils.checkPropExist(key, schema)) {
                logicalCompareBuilder.setPropId(SchemaUtils.getPropId(key, schema));
            } else {
                logicalCompareBuilder.setPropId(labelIndexList.getOrDefault(key, TreeConstants.MAGIC_PROP_ID));
            }
        }

        if (hasContainer.getPredicate() == null) {
            logicalCompareBuilder.setCompare(Message.CompareType.EXIST);
        } else {
            if (StringUtils.equals(key, T.label.getAccessor())) {
                logicalCompareBuilder.setCompare(parseCompareType(hasContainer.getPredicate(), hasContainer.getBiPredicate()));
                if (hasContainer.getBiPredicate() instanceof Compare) {
                    String value = hasContainer.getValue().toString();
                    int labelId;
                    try {
                        labelId = schema.getElement(value).getLabelId();
                    } catch (Exception ignored) {
                        labelId = TreeConstants.MAGIC_LABEL_ID;
                    }
                    logicalCompareBuilder.setValue(Message.Value.newBuilder()
                            .setIntValue(labelId)
                            .setValueType(Message.VariantType.VT_INT))
                            .setType(Message.VariantType.VT_INT);
                } else if (hasContainer.getBiPredicate() instanceof Contains) {
                    Collection<String> labelIdList = (Collection<String>) hasContainer.getValue();
                    Message.Value.Builder valueBuilder = Message.Value.newBuilder().setValueType(Message.VariantType.VT_INT_LIST);
                    labelIdList.forEach(v -> {
                        try {
                            GraphElement element = schema.getElement(v);
                            valueBuilder.addIntValueList(element.getLabelId());
                        } catch (Exception ignored) {
                        }
                    });
                    logicalCompareBuilder.setValue(valueBuilder).setType(Message.VariantType.VT_INT_LIST);
                } else {
                    throw new IllegalArgumentException("label compare => " + hasContainer.getBiPredicate());
                }
            } else if (StringUtils.equals(key, T.id.getAccessor())) {
                logicalCompareBuilder.setCompare(parseCompareType(hasContainer.getPredicate(), hasContainer.getBiPredicate()));
                if (hasContainer.getBiPredicate() instanceof Compare) {
                    logicalCompareBuilder.setValue(
                            Message.Value.newBuilder()
                                    .setLongValue(parseLongIdValue(hasContainer.getValue(), true))
                                    .setValueType(Message.VariantType.VT_LONG)
                    ).setType(Message.VariantType.VT_LONG);
                } else if (hasContainer.getBiPredicate() instanceof Contains) {
                    Collection<Object> ids = (Collection<Object>) hasContainer.getValue();
                    logicalCompareBuilder.setValue(
                            Message.Value.newBuilder()
                                    .addAllLongValueList(
                                            ids.stream()
                                                    .map(v -> parseLongIdValue(v, true))
                                                    .collect(Collectors.toList()))
                                    .setValueType(Message.VariantType.VT_LONG_LIST)
                    ).setType(Message.VariantType.VT_LONG_LIST);
                } else {
                    throw new IllegalArgumentException("id compare =>" + hasContainer.getBiPredicate());
                }
            } else {
                P<?> predicate = hasContainer.getPredicate();
                parsePredicateLogicalCompare(logicalCompareBuilder, predicate);
            }
        }

        return logicalCompareBuilder.build();
    }

    public static void parsePredicateLogicalCompare(Message.LogicalCompare.Builder logicalCompareBuilder, P predicate) {
        if (predicate instanceof AndP) {
            List<P> plist = ((AndP) predicate).getPredicates();
            logicalCompareBuilder.setCompare(Message.CompareType.AND_RELATION);
            int andPropId = logicalCompareBuilder.getPropId();
            for (P p : plist) {
                Message.LogicalCompare.Builder childCompareBuilder = Message.LogicalCompare.newBuilder()
                        .setPropId(andPropId);
                parsePredicateLogicalCompare(childCompareBuilder, p);
                logicalCompareBuilder.addChildCompareList(childCompareBuilder);
            }
        } else if (predicate instanceof OrP) {
            List<P> plist = ((OrP) predicate).getPredicates();
            logicalCompareBuilder.setCompare(Message.CompareType.OR_RELATION);
            int orPropId = logicalCompareBuilder.getPropId();
            for (P p : plist) {
                Message.LogicalCompare.Builder childCompareBuilder = Message.LogicalCompare.newBuilder()
                        .setPropId(orPropId);
                parsePredicateLogicalCompare(childCompareBuilder, p);
                logicalCompareBuilder.addChildCompareList(childCompareBuilder);
            }
        } else {
            BiPredicate biPredicate = predicate instanceof CustomPredicate ? null : predicate.getBiPredicate();
            Object value = predicate instanceof CustomPredicate ? null : predicate.getValue();
            Pair<Message.CompareType, Message.Value.Builder> compareValuePair = parseCompareValuePair(predicate, biPredicate, value);
            if (predicate instanceof NegatePredicate && ((NegatePredicate) predicate).isNegate()) {
                compareValuePair.getRight().setBoolFlag(true);
            }
            logicalCompareBuilder.setCompare(compareValuePair.getLeft())
                    .setValue(compareValuePair.getRight())
                    .setType(compareValuePair.getRight().getValueType());
        }
    }

    /**
     * Get {@link Message.CompareType} from {@link BiPredicate}
     *
     * @param biPredicate The given BiPredicate instance
     * @return The CompareType result
     */
    private static Pair<Message.CompareType, Message.Value.Builder> parseCompareValuePair(Predicate predicate, BiPredicate<?, ?> biPredicate, Object value) {
        if (null == predicate && null == value) {
            return Pair.of(Message.CompareType.EXIST, Message.Value.newBuilder());
        }

        if (predicate instanceof RegexPredicate) {
            return Pair.of(
                    Message.CompareType.REGEX,
                    parseValueBuilder(((RegexPredicate) predicate).getRegex()));
        } else if (predicate instanceof StringPredicate) {
            MatchType matchType = ((StringPredicate) predicate).getMatchType();
            return Pair.of(
                    Message.CompareType.valueOf(StringUtils.upperCase(matchType.name())),
                    parseValueBuilder(((StringPredicate) predicate).getContent()));
        } else if (predicate instanceof ListPredicate) {
            ListMatchType listMatchType = ((ListPredicate) predicate).getMatchType();
            return Pair.of(
                    Message.CompareType.valueOf(StringUtils.upperCase(listMatchType.name())),
                    parseValueBuilder(((ListPredicate) predicate).getListValue()));
        } else if (predicate instanceof DimPredicate) {
            DimMatchType dimMatchType = ((DimPredicate) predicate).getDimMatchType();
            return Pair.of(
                    Message.CompareType.valueOf((StringUtils.upperCase(dimMatchType.name()))),
                    parseValueBuilder(((DimPredicate) predicate).getDimTable()));
        } else if (biPredicate instanceof Compare) {
            Compare compare = (Compare) biPredicate;
            return Pair.of(
                    Message.CompareType.valueOf(StringUtils.upperCase(compare.name())),
                    parseValueBuilder(value));
        } else if (biPredicate instanceof Contains) {
            Contains contains = (Contains) biPredicate;
            return Pair.of(
                    Message.CompareType.valueOf(StringUtils.upperCase(contains.name())),
                    parseValueBuilder(value));
        } else {
            throw new UnsupportedOperationException(biPredicate.toString());
        }
    }

    private static Message.Value.Builder parseValueBuilder(Object value) {
        if (null == value) {
            throw new IllegalArgumentException("Cant parse null value");
        }

        Class<?> clazz = value.getClass();
        Message.VariantType variantType = parseVariantType(clazz, value);
        return parseValueBuilderFromType(value, variantType);
    }

    /**
     * Get {@link Message.CompareType} from {@link BiPredicate}
     *
     * @param biPredicate The given BiPredicate instance
     * @return The CompareType result
     */
    public static Message.CompareType parseCompareType(Predicate predicate, BiPredicate<?, ?> biPredicate) {
        if (predicate instanceof RegexPredicate) {
            return Message.CompareType.REGEX;
        } else if (predicate instanceof StringPredicate) {
            MatchType matchType = StringPredicate.class.cast(predicate).getMatchType();
            return Message.CompareType.valueOf(StringUtils.upperCase(matchType.name()));
        } else if (predicate instanceof ListPredicate) {
            ListMatchType listMatchType = ListPredicate.class.cast(predicate).getMatchType();
            return Message.CompareType.valueOf(StringUtils.upperCase(listMatchType.name()));
        } else if (biPredicate instanceof Compare) {
            Compare compare = Compare.class.cast(biPredicate);
            return Message.CompareType.valueOf(StringUtils.upperCase(compare.name()));
        } else if (biPredicate instanceof Contains) {
            Contains contains = Contains.class.cast(biPredicate);
            return Message.CompareType.valueOf(StringUtils.upperCase(contains.name()));
        } else {
            throw new UnsupportedOperationException(biPredicate.toString());
        }
    }


    /**
     * Serialize the given value to byte[]
     *
     * @param value       The given value
     * @param variantType The value type
     * @return The byte[] result
     */
    private static Message.Value.Builder parseValueBuilderFromType(Object value, Message.VariantType variantType) {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder().setValueType(variantType);
        switch (variantType) {
            case VT_INT:
                valueBuilder.setIntValue(Integer.class.cast(value));
                break;
            case VT_LONG:
                valueBuilder.setLongValue(Long.class.cast(value));
                break;
            case VT_FLOAT:
                valueBuilder.setFloatValue(Float.class.cast(value));
                break;
            case VT_DOUBLE:
                valueBuilder.setDoubleValue(Double.class.cast(value));
                break;
            case VT_STRING:
                valueBuilder.setStrValue(String.class.cast(value));
                break;
            case VT_STRING_LIST:
                valueBuilder.addAllStrValueList((List<String>) value);
                break;
            case VT_LONG_LIST:
                valueBuilder.addAllLongValueList((List<Long>) value);
                break;
            case VT_INT_LIST:
                valueBuilder.addAllIntValueList((List<Integer>) value);
                break;
            default:
                throw new UnsupportedOperationException("value=>" + value + " type=>" + variantType.toString());
        }

        return valueBuilder;
    }


    /**
     * Get {@link Message.VariantType} from Class<\?>
     *
     * @param valueClazz The given clazz
     * @return The VariantType instance
     */
    public static Message.VariantType parseVariantType(Class<?> valueClazz, Object value) {
        if (value instanceof List) {
            List<Object> valueList = List.class.cast(value);
            Object valueObject = valueList.get(0);
            String className = valueObject.getClass().getSimpleName();
            if (className.contains("Integer")) {
                className = className.replace("Integer", "Int");
            }
            return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(className) + "_LIST");
        } else if (StringUtils.contains(value.getClass().toString(), "PickTokenKey")) {
            Number number = ReflectionUtils.getFieldValue(BranchStep.class.getDeclaredClasses()[0], value, "number");
            return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(number.getClass().getSimpleName()));
        } else {
            String className = valueClazz.getSimpleName();
            if (className.contains("Integer")) {
                className = className.replace("Integer", "Int");
            }
            return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(className));
        }
    }

    public static Message.VariantType parseVariantFromDataType(DataType dataType) {
        return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(dataType.name()));
    }

    public static com.alibaba.maxgraph.sdkcommon.meta.DataType parseDataTypeFromPropDataType(DataType dataType) {
        try {
            switch (dataType) {
                case INT: {
                    return com.alibaba.maxgraph.sdkcommon.meta.DataType.INT;
                }
                case INT_LIST: {
                    com.alibaba.maxgraph.sdkcommon.meta.DataType type = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                    type.setExpression("INT");
                    return type;
                }
                case LONG_LIST: {
                    com.alibaba.maxgraph.sdkcommon.meta.DataType type = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                    type.setExpression("LONG");
                    return type;
                }
                case FLOAT_LIST: {
                    com.alibaba.maxgraph.sdkcommon.meta.DataType type = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                    type.setExpression("FLOAT");
                    return type;
                }
                case DOUBLE_LIST: {
                    com.alibaba.maxgraph.sdkcommon.meta.DataType type = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                    type.setExpression("DOUBLE");
                    return type;
                }
                case STRING_LIST: {
                    com.alibaba.maxgraph.sdkcommon.meta.DataType type = new com.alibaba.maxgraph.sdkcommon.meta.DataType(InternalDataType.LIST);
                    type.setExpression("STRING");
                    return type;
                }
                case BYTES: {
                    return com.alibaba.maxgraph.sdkcommon.meta.DataType.BYTES;
                }
                default: {
                    return com.alibaba.maxgraph.sdkcommon.meta.DataType.valueOf(dataType.name());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the source tree node for given tree node
     *
     * @param treeNode The given tree node
     * @return The source tree node
     */
    public static TreeNode getSourceTreeNode(TreeNode treeNode) {
        TreeNode currentNode = treeNode;
        while (currentNode instanceof UnaryTreeNode) {
            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }

        return currentNode;
    }

    /**
     * Travel the given tree node from end to start, and there's flat map node and no aggregate after it,
     * it means there would be multiple result for one input value
     *
     * @param treeNode The given tree node
     * @return The check results
     */
    public static boolean checTreeFlatMapLastFlag(TreeNode treeNode) {
        TreeNode currentNode = treeNode;
        boolean aggregate = false;
        while (currentNode instanceof UnaryTreeNode) {
            if (currentNode.getNodeType() == NodeType.AGGREGATE) {
                aggregate = true;
            } else if (currentNode.getNodeType() == NodeType.FLATMAP) {
                if (aggregate) {
                    return false;
                } else {
                    return true;
                }
            }
            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }

        return false;
    }

    /**
     * Travel the given tree node from end to start, and there's map node and no aggregate after it,
     * it means there would be multiple result for one input value
     *
     * @param treeNode The given tree node
     * @return The check results
     */
    public static boolean checkTreeMapLastFlag(TreeNode treeNode) {
        TreeNode currentNode = treeNode;
        boolean aggregate = false;
        while (currentNode instanceof UnaryTreeNode) {
            if (currentNode.getNodeType() == NodeType.AGGREGATE) {
                aggregate = true;
            } else if (currentNode.getNodeType() == NodeType.MAP || currentNode.getNodeType() == NodeType.FILTER) {
                if (aggregate) {
                    return false;
                } else {
                    return true;
                }
            }
            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }

        return false;
    }

    /**
     * Travel the given tree node from end to start, and there's aggregate node
     *
     * @param treeNode The given tree node
     * @return The check results
     */
    public static boolean checkTreeAggregateExistFlag(TreeNode treeNode) {
        TreeNode currentNode = treeNode;
        while (currentNode instanceof UnaryTreeNode) {
            if (currentNode.getNodeType() == NodeType.AGGREGATE) {
                return true;
            }
            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }

        return false;
    }

    /**
     * There're all map/filter in tree node
     */
    public static boolean checkAllMapFilterNode(TreeNode treeNode) {
        TreeNode currentNode = treeNode;
        while (currentNode instanceof UnaryTreeNode) {
            if (currentNode.getNodeType() == NodeType.AGGREGATE) {
                if (currentNode != treeNode) {
                    return true;
                }
            }
            if (currentNode.getNodeType() != NodeType.FILTER
                    && currentNode.getNodeType() != NodeType.MAP) {
                return false;
            }

            currentNode = UnaryTreeNode.class.cast(currentNode).getInputNode();
        }

        return true;
    }

    /**
     * Get the output value type with tree node list and pop
     *
     * @param treeNodeList The given tree node list
     * @param pop          The given pop
     * @return The output value type
     */
    public static ValueType parseValueTypeWithPop(List<TreeNode> treeNodeList, Pop pop) {
        if (treeNodeList == null || treeNodeList.isEmpty()) {
            return null;
        }
        if (pop == null) {
            Set<ValueType> valueTypeSet = Sets.newHashSet();
            treeNodeList.forEach(v -> valueTypeSet.add(v.getOutputValueType()));
            List<ValueType> valueTypeList = Lists.newArrayList(valueTypeSet);
            if (valueTypeList.size() == 1) {
                return valueTypeList.get(0);
            } else {
                return new ListValueType(new VarietyValueType(valueTypeSet));
            }
        } else {
            switch (pop) {
                case first: {
                    return treeNodeList.get(0).getOutputValueType();
                }
                case last: {
                    return treeNodeList.get(treeNodeList.size() - 1).getOutputValueType();
                }
                default: {
                    Set<ValueType> valueTypeSet = Sets.newHashSet();
                    treeNodeList.forEach(v -> valueTypeSet.add(v.getOutputValueType()));
                    return new ListValueType(valueTypeSet.size() == 1 ? Lists.newArrayList(valueTypeSet).get(0) : new VarietyValueType(valueTypeSet));
                }
            }
        }
    }

    public static int getPropertyId(GraphSchema schema, String propKey) {
        if (SchemaUtils.checkPropExist(propKey, schema)) {
            return SchemaUtils.getPropId(propKey, schema);
        } else {
            return TreeConstants.MAGIC_PROP_ID;
        }
    }

    public static QueryFlowOuterClass.OperatorType parseJoinOperatorType(TreeNode treeNode) {
        if (treeNode instanceof CountGlobalTreeNode) {
            return QueryFlowOuterClass.OperatorType.JOIN_COUNT_LABEL;
        } else if (treeNode instanceof SumTreeNode) {
            SumTreeNode sumTreeNode = SumTreeNode.class.cast(treeNode);
            if (sumTreeNode.isJoinZeroFlag()) {
                return QueryFlowOuterClass.OperatorType.JOIN_COUNT_LABEL;
            }
        }

        return QueryFlowOuterClass.OperatorType.JOIN_LABEL;
    }

    public static Object convertValueWithType(Object value, Message.VariantType dataType) {
        switch (dataType) {
            case VT_INT:
                return Integer.parseInt(value.toString());
            case VT_LONG:
                return Long.parseLong(value.toString());
            case VT_FLOAT:
                return Float.parseFloat(value.toString());
            case VT_DOUBLE:
                return Double.parseDouble(value.toString());
            case VT_STRING:
                return value.toString();
            case VT_INT_LIST:
                if (value instanceof Collection) {
                    return ((Collection) value).stream().map(v -> Integer.parseInt(v.toString())).collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Can't convert value=>" + value + " to List<Integer>");
                }
            case VT_LONG_LIST:
                if (value instanceof Collection) {
                    return ((Collection) value).stream().map(v -> Long.parseLong(v.toString())).collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Can't convert value=>" + value + " to List<Long>");
                }
            case VT_STRING_LIST:
                if (value instanceof Collection) {
                    return ((Collection) value).stream().map(v -> v.toString()).collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Can't convert value=>" + value + " to List<String>");
                }
            case VT_FLOAT_LIST:
                if (value instanceof Collection) {
                    return ((Collection) value).stream().map(v -> Float.parseFloat(v.toString())).collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Cant convert value=>" + value + " to List<Float>");
                }
            case VT_DOUBLE_LIST:
                if (value instanceof Collection) {
                    return ((Collection) value).stream().map(v -> Double.parseDouble(v.toString())).collect(Collectors.toList());
                } else {
                    throw new IllegalArgumentException("Cant convert value=>" + value + " to List<Double>");
                }
            default:
                throw new IllegalArgumentException(value.toString() + " " + dataType);
        }
    }

    public static QueryFlowOuterClass.InputShuffleType parseShuffleTypeFromEdge(LogicalEdge logicalEdge) {
        return QueryFlowOuterClass.InputShuffleType.valueOf(StringUtils.upperCase(logicalEdge.getShuffleType().name() + "_TYPE"));
    }

    public static QueryFlowOuterClass.RuntimeGraphSchemaProto buildRuntimeGraphSchema(GraphSchema schema) {
        QueryFlowOuterClass.RuntimeGraphSchemaProto.Builder schemaBuilder = QueryFlowOuterClass.RuntimeGraphSchemaProto.newBuilder();
        for (GraphVertex vertex : schema.getVertexList()) {
            schemaBuilder.addVertexTypes(QueryFlowOuterClass.RuntimeVertexTypeProto.newBuilder()
                    .setLabelId(vertex.getLabelId())
                    .setLabelName(vertex.getLabel())
                    .addAllPrimaryKeys(vertex.getPrimaryKeyList().stream().map(GraphProperty::getName)
                            .collect(Collectors.toList()))
                    .addAllProperties(vertex.getPropertyList().stream().map(p ->
                            QueryFlowOuterClass.RuntimePropertyProto.newBuilder()
                                    .setId(p.getId())
                                    .setName(p.getName())
                                    .setDataType(parseDataTypeFromPropDataType(p.getDataType()).name()).build()
                    ).collect(Collectors.toList())).build());
        }
        for (GraphEdge edge : schema.getEdgeList()) {
            schemaBuilder.addEdgeTypes(QueryFlowOuterClass.RuntimeEdgeTypeProto.newBuilder()
                    .setLabelId(edge.getLabelId())
                    .setLabelName(edge.getLabel())
                    .addAllProperties(edge.getPropertyList().stream().map(p ->
                            QueryFlowOuterClass.RuntimePropertyProto.newBuilder()
                                    .setId(p.getId())
                                    .setName(p.getName())
                                    .setDataType(parseDataTypeFromPropDataType(p.getDataType()).name()).build())
                            .collect(Collectors.toList()))
                    .addAllRelations(edge.getRelationList().stream()
                            .map(r -> QueryFlowOuterClass.RuntimeEdgeRelationProto.newBuilder()
                                    .setSourceLabel(r.getSource().getLabel())
                                    .setTargetLabel(r.getTarget().getLabel()).build())
                            .collect(Collectors.toList()))
                    .build());
        }
        return schemaBuilder.build();
    }
}
