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
import com.alibaba.maxgraph.Message.LogicalCompare;
import com.alibaba.maxgraph.common.util.SchemaUtils;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.tree.TreeConstants;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.CustomPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListMatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.MatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.PredicateType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.NegatePredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.RegexPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.StringPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimMatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimTable;
import com.alibaba.maxgraph.compiler.tree.value.ElementType;
import com.alibaba.maxgraph.compiler.tree.value.ElementValueType;
import com.alibaba.maxgraph.compiler.tree.value.MapEntryValueType;
import com.alibaba.maxgraph.compiler.optimizer.CompilerConfig;
import com.alibaba.maxgraph.compiler.optimizer.OperatorListManager;
import com.alibaba.maxgraph.compiler.tree.value.ListValueType;
import com.alibaba.maxgraph.compiler.tree.value.MapValueType;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.tree.value.VarietyValueType;

import com.alibaba.maxgraph.compiler.prepare.store.PrepareCompareEntity;
import com.alibaba.maxgraph.compiler.prepare.store.PrepareEntity;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Expression/Function related util
 */
public class MaxGraphUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MaxGraphUtils.class);

    /**
     * Parse {@link HasContainer} list to {@link LogicalCompare} list
     *
     * @param hasContainers The given HasContainer list
     * @return The LogicalCompare list
     */
    public static List<LogicalCompare> parseLogicalCompareList(
            List<HasContainer> hasContainers,
            GraphSchema schema,
            OperatorListManager operatorListManager,
            Map<String, Integer> labelIndexMap,
            CompilerConfig compilerConfig) {
        List<LogicalCompare> logicalCompareList = Lists.newArrayList();
        for (HasContainer hasContainer : hasContainers) {
            parseLogicalCompare(hasContainer.getKey(), hasContainer.getPredicate(), schema, logicalCompareList, operatorListManager, labelIndexMap, compilerConfig);
        }

        return logicalCompareList;
    }

    /**
     * Parse HasContainer to LogicalCompare
     *
     * @param key       The compare key
     * @param predicate The predicate
     * @param schema    The graph schema
     */
    private static void parseLogicalCompare(
            String key, P<?> predicate,
            GraphSchema schema,
            List<LogicalCompare> logicalCompareList,
            OperatorListManager operatorListManager,
            Map<String, Integer> labelIndexMap,
            CompilerConfig compilerConfig) {
        if (predicate instanceof OrP) {
            throw new UnsupportedOperationException("or");
        } else if (predicate instanceof AndP) {
            AndP andP = AndP.class.cast(predicate);
            List<P> predicateList = andP.getPredicates();
            for (P p : predicateList) {
                parseLogicalCompare(key, p, schema, logicalCompareList, operatorListManager, labelIndexMap, compilerConfig);
            }
        } else {
            Object value = parsePredicateValue(predicate);
            BiPredicate biPredicate = null == predicate ? null : (predicate instanceof CustomPredicate ? null : predicate.getBiPredicate());
            boolean isNegate = predicate instanceof NegatePredicate && ((NegatePredicate) predicate).isNegate();
            LogicalCompare.Builder builder = LogicalCompare.newBuilder();
            if (value != null &&
                    (OperatorListManager.isPrepareValue(key)
                            || OperatorListManager.isPrepareValue(value.toString()))) {
                int prepareKeyIndex = PrepareEntity.CONSTANT_INDEX;
                if (OperatorListManager.isPrepareValue(key)) {
                    prepareKeyIndex = OperatorListManager.getPrepareValue(key);
                    checkArgument(prepareKeyIndex > 0, "Compare key index must > 0 for " + key);
                }

                int prepareValueIndex = PrepareEntity.CONSTANT_INDEX;
                if (OperatorListManager.isPrepareValue(value.toString())) {
                    prepareValueIndex = OperatorListManager.getPrepareValue(value.toString());
                    checkArgument(prepareValueIndex > 0, "Compare key index must > 0 for " + value);
                }

                int argumentIndex = operatorListManager.getAndIncrementArgumentIndex();
                builder.setIndex(argumentIndex);

                PrepareCompareEntity prepareCompareEntity = new PrepareCompareEntity(argumentIndex, prepareKeyIndex, prepareValueIndex, parseCompareType(predicate, biPredicate));
                prepareCompareEntity.setNegate(isNegate);
                if (prepareKeyIndex == PrepareEntity.CONSTANT_INDEX) {
                    prepareCompareEntity.setCompareKey(parsePropIdByKey(key, schema));
                }
                if (prepareValueIndex == PrepareEntity.CONSTANT_INDEX) {
                    prepareCompareEntity.setCompareValue(parsePropLabelValue(key, schema, value));
                }
                operatorListManager.addPrepareEntity(prepareCompareEntity);
            } else {
                if (StringUtils.isNotEmpty(key)) {
                    int propId = parsePropLabelIdByKey(key, schema, labelIndexMap);
                    builder.setPropId(propId);
                }
                value = parsePropLabelValue(key, schema, value);
                Message.VariantType variantType = null == value ? Message.VariantType.VT_UNKNOWN : parseVariantType(value.getClass(), value);
                Message.Value.Builder valueBuilder = null == value ? Message.Value.newBuilder() : createValueFromType(value, variantType, compilerConfig);
                valueBuilder.setBoolFlag(isNegate);
                builder.setCompare((null == value && null == predicate) ? Message.CompareType.EXIST : parseCompareType(predicate, biPredicate))
                        .setValue(valueBuilder.build())
                        .setType(variantType);
            }

            logicalCompareList.add(builder.build());
        }
    }

    private static Object parsePredicateValue(P<?> predicate) {
        PredicateType predicateType = PredicateType.GREMLIN;
        if (predicate instanceof CustomPredicate) {
            predicateType = CustomPredicate.class.cast(predicate).getPredicateType();
        }
        switch (predicateType) {
            case REGEX: {
                return RegexPredicate.class.cast(predicate).getRegex();
            }
            case STRING: {
                return StringPredicate.class.cast(predicate).getContent();
            }
            case LIST: {
                return ListPredicate.class.cast(predicate).getListValue();
            }
            case DIM: {
                return DimPredicate.class.cast(predicate).getDimTable();
            }
            default: {
                return predicate == null ? null : predicate.getValue();
            }
        }
    }

    public static int parsePropLabelIdByKey(String key, GraphSchema schema, Map<String, Integer> labelIndexMap) {
        try {
            return parsePropIdByKeyOption(key, schema);
        } catch (Exception e) {
            try {
                return parseLabelIdByKeyOption(key, labelIndexMap);
            } catch (Exception ee) {
                return TreeConstants.MAGIC_LABEL_ID;
            }
        }
    }

    public static List<Integer> parsePropLabelIdByKey(Collection<String> keyList, GraphSchema schema, Map<String, Integer> labelIndexMap) {
        List<Integer> keyIndexList = Lists.newArrayList();
        for (String keyVal : keyList) {
            keyIndexList.add(parsePropLabelIdByKey(keyVal, schema, labelIndexMap));
        }

        return keyIndexList;
    }

    private static Object parsePropLabelValue(String key, GraphSchema schema, Object value) {
        Object resultValue;
        if (StringUtils.equals(key, T.label.getAccessor())) {
            if (value instanceof Collection) {
                resultValue = Lists.newArrayList();
                for (Object labelObject : Collection.class.cast(value)) {
                    List.class.cast(resultValue).add(schema.getElement(String.class.cast(labelObject)).getLabelId());
                }
            } else {
                resultValue = schema.getElement(String.class.cast(value)).getLabelId();
            }
        } else if (StringUtils.equals(key, T.id.getAccessor())) {
            if (value instanceof Collection) {
                List<Long> idList = Lists.newArrayList();
                for (Object idObj : Collection.class.cast(value)) {
                    idList.add(parseIdLongValue(idObj.toString()));
                }
                resultValue = idList;
            } else if (value instanceof DimTable) {
                resultValue = value;
            } else {
                resultValue = parseIdLongValue(value);
            }
        } else {
            resultValue = value;
        }

        return resultValue;
    }

    private static long parseIdLongValue(Object object) {
        String strValue = object.toString();
        long value;
        if (StringUtils.contains(strValue, ".")) {
            value = Long.parseLong(StringUtils.substring(strValue, StringUtils.indexOf(strValue, ".") + 1));
        } else {
            value = Long.parseLong(strValue);
        }
        return value;
    }

    public static int parsePropIdByKey(String key, GraphSchema schema) {
        try {
            return parsePropIdByKeyOption(key, schema);
        } catch (Exception e) {
            return TreeConstants.MAGIC_PROP_ID;
        }
    }

    private static int parsePropIdByKeyOption(String key, GraphSchema schema) {
        int propId;
        if (StringUtils.equals(key, T.id.getAccessor())) {
            propId = TreeConstants.ID_INDEX;
        } else if (StringUtils.equals(key, T.label.getAccessor())) {
            propId = TreeConstants.LABEL_INDEX;
        } else {
            propId = SchemaUtils.getPropId(key, schema);
        }

        return propId;
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
            return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(valueObject.getClass().getSimpleName()) + "_LIST");
        } else {
            return Message.VariantType.valueOf("VT_" + StringUtils.upperCase(valueClazz.getSimpleName()));
        }
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
        } else if (predicate instanceof DimPredicate) {
            DimMatchType dimMatchType = DimPredicate.class.cast(predicate).getDimMatchType();
            return Message.CompareType.valueOf((StringUtils.upperCase(dimMatchType.name())));
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
    public static Message.Value.Builder createValueFromType(Object value, Message.VariantType variantType, CompilerConfig compilerConfig) {
        Message.Value.Builder valueBuilder = Message.Value.newBuilder();
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
     * Create operator type from function type and function requirement list
     *
     * @param functionType            The given function type
     * @param functionRequirementList The given function requirements
     * @return The operator type
     */
//    public static QueryFlowOuterClass.OperatorType parseOperatorType(FunctionType functionType, Set<FunctionRequirement> functionRequirementList) {
//        String operatorTypeValue = functionType.toString();
//        if (null != functionRequirementList && !functionRequirementList.isEmpty()) {
//            List<FunctionRequirement> functionRequirements = Lists.newArrayList(functionRequirementList);
//            Collections.sort(functionRequirements);
//            operatorTypeValue = operatorTypeValue + "_" + org.apache.commons.lang.StringUtils.join(functionRequirements, "_");
//        }
//        return QueryFlowOuterClass.OperatorType.valueOf(operatorTypeValue);
//    }


    /**
     * Convert pop to pop type in proto
     *
     * @param pop The given pop
     * @return The pop type
     */
    public static Message.PopType parsePopType(Pop pop) {
        return Message.PopType.valueOf(StringUtils.upperCase(pop.name()));
    }

    /**
     * Convert label and comparator to OrderComparator
     *
     * @param labelIndex The given label index
     * @param order      The given Comparator
     * @return The result OrderComparator
     */
    public static Message.OrderComparator.Builder parseOrderComparator(int labelIndex, Order order) {
        Message.OrderComparator.Builder comparatorBuilder = Message.OrderComparator.newBuilder();
        comparatorBuilder.setPropId(labelIndex);
        comparatorBuilder.setOrderType(Message.OrderType.valueOf(StringUtils.upperCase(order.name())));

        return comparatorBuilder;
    }

    /**
     * Union left and right value type to output value type
     *
     * @param leftType  The given left value type
     * @param rightType The given right value type
     * @return The result value type
     */
    public static ValueType unionValueType(ValueType leftType, ValueType rightType) {
        Set<ValueType> valueTypeSet = Sets.newHashSet();
        if (leftType instanceof VarietyValueType) {
            valueTypeSet.addAll(((VarietyValueType) leftType).getValueTypeList());
        } else {
            valueTypeSet.add(leftType);
        }

        if (rightType instanceof VarietyValueType) {
            valueTypeSet.addAll(((VarietyValueType) rightType).getValueTypeList());
        } else {
            valueTypeSet.add(rightType);
        }

        List<ValueType> valueTypeList = Lists.newArrayList(valueTypeSet);
        if (valueTypeList.size() > 1) {
            return new VarietyValueType(valueTypeList);
        } else {
            return valueTypeList.get(0);
        }
    }

    /**
     * Unfold the given value type
     *
     * @param valueType The given value type
     * @return The result value type
     */
    public static ValueType unfoldValueType(ValueType valueType) {
        ValueType unfoldType;
        if (valueType instanceof ListValueType) {
            unfoldType = ListValueType.class.cast(valueType).getListValue();
        } else if (valueType instanceof MapValueType) {
            MapValueType mapValueType = MapValueType.class.cast(valueType);
            unfoldType = new MapEntryValueType(mapValueType.getKey(), mapValueType.getValue());
        } else {
            unfoldType = valueType;
        }

        return unfoldType;
    }

    /**
     * Parse property value proto to property value result
     *
     * @param variantType The value type
     * @param byteString  The value payload
     * @return The property value result
     */
    public static Object parsePropertyValueResult(Message.VariantType variantType, ByteString byteString) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteString.toByteArray());
        Object value;
        switch (variantType) {
            case VT_INT:
                value = byteBuffer.getInt();
                break;
            case VT_LONG:
                value = byteBuffer.getLong();
                break;
            case VT_STRING:
                value = new String(byteBuffer.array());
                break;
            case VT_FLOAT:
                value = byteBuffer.getFloat();
                break;
            case VT_DOUBLE:
                value = byteBuffer.getDouble();
                break;
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
            case VT_STRING_LIST: {
                try {
                    Message.ListString listStringValue = Message.ListString.parseFrom(byteString);
                    List<String> stringValueList = Lists.newArrayList();
                    listStringValue.getValueList().asByteStringList().forEach(v -> stringValueList.add(new String(v.toByteArray())));
                    return stringValueList;
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
            default:
                throw new UnsupportedOperationException(variantType.toString());
        }

        return value;
    }

    private static int parseLabelIdByKeyOption(String labelName, Map<String, Integer> labelIndexMap) {
        return labelIndexMap.get(labelName);
    }

    private static ValueType parseValueTypeFromDataType(Message.VariantType variantType) {
        switch (variantType) {
            case VT_INT_LIST:
                return new ListValueType(new ElementValueType(ElementType.VALUE, Message.VariantType.VT_INT));
            case VT_LONG_LIST:
                return new ListValueType(new ElementValueType(ElementType.VALUE, Message.VariantType.VT_LONG));
            case VT_STRING_LIST:
                return new ListValueType(new ElementValueType(ElementType.VALUE, Message.VariantType.VT_STRING));
            default:
                return new ElementValueType(ElementType.VALUE, variantType);
        }
    }

    public static Message.VariantType parsePropertyDataType(String propKey, GraphSchema schema) {
        try {
            Set<DataType> dataTypeSet = SchemaUtils.getPropDataTypeList(propKey, schema);
            if (dataTypeSet.size() != 1) {
                throw new IllegalArgumentException("invalid data type " + dataTypeSet + " for prop " + propKey);
            }
            return CompilerUtils.parseVariantFromDataType(dataTypeSet.iterator().next());
        } catch (Exception e) {
            LOG.warn("There's no property=>" + propKey);
            return Message.VariantType.VT_UNKNOWN;
        }
    }
}
