package com.alibaba.maxgraph.v2.frontend.compiler.utils;

import com.alibaba.maxgraph.proto.v2.CompareType;
import com.alibaba.maxgraph.proto.v2.InputShuffleType;
import com.alibaba.maxgraph.proto.v2.LogicalCompare;
import com.alibaba.maxgraph.proto.v2.OperatorType;
import com.alibaba.maxgraph.proto.v2.Value;
import com.alibaba.maxgraph.proto.v2.VariantType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphSchema;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.SchemaElement;
import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.compiler.logical.LogicalEdge;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.CountGlobalTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.NodeType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.SumTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.TreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.UnaryTreeNode;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.v2.frontend.compiler.tree.value.VarietyValueType;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public static LogicalCompare parseLogicalCompare(HasContainer hasContainer, GraphSchema schema, Map<String, Integer> labelIndexList, boolean vertexFlag) {
        LogicalCompare.Builder logicalCompareBuilder = LogicalCompare.newBuilder();
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
            logicalCompareBuilder.setCompare(CompareType.EXIST);
        } else {
            if (StringUtils.equals(key, T.label.getAccessor())) {
                logicalCompareBuilder.setCompare(parseCompareType(hasContainer.getPredicate(), hasContainer.getBiPredicate()));
                if (hasContainer.getBiPredicate() instanceof Compare) {
                    String value = hasContainer.getValue().toString();
                    int labelId;
                    try {
                        labelId = schema.getSchemaElement(value).getLabelId();
                    } catch (Exception ignored) {
                        labelId = TreeConstants.MAGIC_LABEL_ID;
                    }
                    logicalCompareBuilder.setValue(Value.newBuilder()
                            .setIntValue(labelId)
                            .setValueType(VariantType.VT_INTEGER))
                            .setType(VariantType.VT_INTEGER);
                } else if (hasContainer.getBiPredicate() instanceof Contains) {
                    Collection<String> labelIdList = (Collection<String>) hasContainer.getValue();
                    Value.Builder valueBuilder = Value.newBuilder().setValueType(VariantType.VT_INTEGER_LIST);
                    labelIdList.forEach(v -> {
                        try {
                            SchemaElement element = schema.getSchemaElement(v);
                            valueBuilder.addIntValueList(element.getLabelId());
                        } catch (Exception ignored) {
                        }
                    });
                    logicalCompareBuilder.setValue(valueBuilder).setType(VariantType.VT_INTEGER_LIST);
                } else {
                    throw new IllegalArgumentException("label compare => " + hasContainer.getBiPredicate());
                }
            } else if (StringUtils.equals(key, T.id.getAccessor())) {
                logicalCompareBuilder.setCompare(parseCompareType(hasContainer.getPredicate(), hasContainer.getBiPredicate()));
                if (hasContainer.getBiPredicate() instanceof Compare) {
                    logicalCompareBuilder.setValue(
                            Value.newBuilder()
                                    .setLongValue(parseLongIdValue(hasContainer.getValue(), true))
                                    .setValueType(VariantType.VT_LONG)
                    ).setType(VariantType.VT_LONG);
                } else if (hasContainer.getBiPredicate() instanceof Contains) {
                    Collection<Object> ids = (Collection<Object>) hasContainer.getValue();
                    logicalCompareBuilder.setValue(
                            Value.newBuilder()
                                    .addAllLongValueList(
                                            ids.stream()
                                                    .map(v -> parseLongIdValue(v, true))
                                                    .collect(Collectors.toList()))
                                    .setValueType(VariantType.VT_LONG_LIST)
                    ).setType(VariantType.VT_LONG_LIST);
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

    public static void parsePredicateLogicalCompare(LogicalCompare.Builder logicalCompareBuilder, P predicate) {
        if (predicate instanceof AndP) {
            List<P> plist = ((AndP) predicate).getPredicates();
            logicalCompareBuilder.setCompare(CompareType.AND_RELATION);
            int andPropId = logicalCompareBuilder.getPropId();
            for (P p : plist) {
                LogicalCompare.Builder childCompareBuilder = LogicalCompare.newBuilder()
                        .setPropId(andPropId);
                parsePredicateLogicalCompare(childCompareBuilder, p);
                logicalCompareBuilder.addChildCompareList(childCompareBuilder);
            }
        } else if (predicate instanceof OrP) {
            List<P> plist = ((OrP) predicate).getPredicates();
            logicalCompareBuilder.setCompare(CompareType.OR_RELATION);
            int orPropId = logicalCompareBuilder.getPropId();
            for (P p : plist) {
                LogicalCompare.Builder childCompareBuilder = LogicalCompare.newBuilder()
                        .setPropId(orPropId);
                parsePredicateLogicalCompare(childCompareBuilder, p);
                logicalCompareBuilder.addChildCompareList(childCompareBuilder);
            }
        } else {
            BiPredicate biPredicate = predicate.getBiPredicate();
            Object value = predicate.getValue();
            Pair<CompareType, Value.Builder> compareValuePair = parseCompareValuePair(predicate, biPredicate, value);
            logicalCompareBuilder.setCompare(compareValuePair.getLeft())
                    .setValue(compareValuePair.getRight())
                    .setType(compareValuePair.getRight().getValueType());
        }
    }

    /**
     * Get {@link CompareType} from {@link BiPredicate}
     *
     * @param biPredicate The given BiPredicate instance
     * @return The CompareType result
     */
    private static Pair<CompareType, Value.Builder> parseCompareValuePair(Predicate predicate, BiPredicate<?, ?> biPredicate, Object value) {
        if (null == predicate && null == value) {
            return Pair.of(CompareType.EXIST, Value.newBuilder());
        }

        if (biPredicate instanceof Compare) {
            Compare compare = (Compare) biPredicate;
            return Pair.of(
                    CompareType.valueOf(StringUtils.upperCase(compare.name())),
                    parseValueBuilder(value));
        } else if (biPredicate instanceof Contains) {
            Contains contains = (Contains) biPredicate;
            return Pair.of(
                    CompareType.valueOf(StringUtils.upperCase(contains.name())),
                    parseValueBuilder(value));
        } else {
            throw new UnsupportedOperationException(biPredicate.toString());
        }
    }

    private static Value.Builder parseValueBuilder(Object value) {
        if (null == value) {
            throw new IllegalArgumentException("Cant parse null value");
        }

        Class<?> clazz = value.getClass();
        VariantType variantType = parseVariantType(clazz, value);
        return parseValueBuilderFromType(value, variantType);
    }

    /**
     * Get {@link CompareType} from {@link BiPredicate}
     *
     * @param biPredicate The given BiPredicate instance
     * @return The CompareType result
     */
    public static CompareType parseCompareType(Predicate predicate, BiPredicate<?, ?> biPredicate) {
        if (biPredicate instanceof Compare) {
            Compare compare = Compare.class.cast(biPredicate);
            return CompareType.valueOf(StringUtils.upperCase(compare.name()));
        } else if (biPredicate instanceof Contains) {
            Contains contains = Contains.class.cast(biPredicate);
            return CompareType.valueOf(StringUtils.upperCase(contains.name()));
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
    private static Value.Builder parseValueBuilderFromType(Object value, VariantType variantType) {
        Value.Builder valueBuilder = Value.newBuilder().setValueType(variantType);
        switch (variantType) {
            case VT_INTEGER:
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
            case VT_INTEGER_LIST:
                valueBuilder.addAllIntValueList((List<Integer>) value);
                break;
            default:
                throw new UnsupportedOperationException("value=>" + value + " type=>" + variantType.toString());
        }

        return valueBuilder;
    }

    /**
     * Get {@link VariantType} from Class<\?>
     *
     * @param valueClazz The given clazz
     * @return The VariantType instance
     */
    public static VariantType parseVariantType(Class<?> valueClazz, Object value) {
        if (value instanceof List) {
            List<Object> valueList = List.class.cast(value);
            Object valueObject = valueList.get(0);
            return VariantType.valueOf("VT_" + StringUtils.upperCase(valueObject.getClass().getSimpleName()) + "_LIST");
        } else if (StringUtils.contains(value.getClass().toString(), "PickTokenKey")) {
            Number number = ReflectionUtils.getFieldValue(BranchStep.class.getDeclaredClasses()[0], value, "number");
            return VariantType.valueOf("VT_" + StringUtils.upperCase(number.getClass().getSimpleName()));
        } else {
            return VariantType.valueOf("VT_" + StringUtils.upperCase(valueClazz.getSimpleName()));
        }
    }

    public static VariantType parseVariantFromDataType(DataType dataType) {
        switch (dataType) {
            case INT: {
                return VariantType.VT_INTEGER;
            }
            case BYTES: {
                return VariantType.VT_BINARY;
            }
            case INT_LIST: {
                return VariantType.VT_INTEGER_LIST;
            }
            default: {
                return VariantType.valueOf("VT_" + StringUtils.upperCase(dataType.name()));
            }
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

    public static OperatorType parseJoinOperatorType(TreeNode treeNode) {
        if (treeNode instanceof CountGlobalTreeNode) {
            return OperatorType.JOIN_COUNT_LABEL;
        } else if (treeNode instanceof SumTreeNode) {
            SumTreeNode sumTreeNode = SumTreeNode.class.cast(treeNode);
            if (sumTreeNode.isJoinZeroFlag()) {
                return OperatorType.JOIN_COUNT_LABEL;
            }
        }

        return OperatorType.JOIN_LABEL;
    }

    public static Object convertValueWithType(Object value, VariantType dataType) {
        switch (dataType) {
            case VT_INTEGER:
                return Integer.parseInt(value.toString());
            case VT_LONG:
                return Long.parseLong(value.toString());
            case VT_FLOAT:
                return Float.parseFloat(value.toString());
            case VT_DOUBLE:
                return Double.parseDouble(value.toString());
            case VT_STRING:
                return value.toString();
            case VT_INTEGER_LIST:
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

    public static InputShuffleType parseShuffleTypeFromEdge(LogicalEdge logicalEdge) {
        return InputShuffleType.valueOf(StringUtils.upperCase(logicalEdge.getShuffleType().name() + "_TYPE"));
    }
}
