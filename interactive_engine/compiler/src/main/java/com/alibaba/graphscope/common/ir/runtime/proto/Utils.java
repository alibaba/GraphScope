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

package com.alibaba.graphscope.common.ir.runtime.proto;

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gaia.proto.*;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra.GroupBy.AggFunc.Aggregate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Int32Value;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * convert {@code RexNode} to data types in protobuf
 */
public abstract class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static final Common.Value protoValue(RexLiteral literal) {
        if (literal.getTypeName() == SqlTypeName.SARG) {
            Sarg sarg = literal.getValueAs(Sarg.class);
            if (!sarg.isPoints()) {
                throw new UnsupportedOperationException(
                        "can not convert continuous ranges to ir core array, sarg=" + sarg);
            }
            List<Comparable> values =
                    com.alibaba.graphscope.common.ir.tools.Utils.getValuesAsList(sarg);
            switch (literal.getType().getSqlTypeName()) {
                case INTEGER:
                    Common.I32Array.Builder i32Array = Common.I32Array.newBuilder();
                    values.forEach(value -> i32Array.addItem(((Number) value).intValue()));
                    return Common.Value.newBuilder().setI32Array(i32Array).build();
                case BIGINT:
                    Common.I64Array.Builder i64Array = Common.I64Array.newBuilder();
                    values.forEach(value -> i64Array.addItem(((Number) value).longValue()));
                    return Common.Value.newBuilder().setI64Array(i64Array).build();
                case CHAR:
                    Common.StringArray.Builder stringArray = Common.StringArray.newBuilder();
                    values.forEach(
                            value ->
                                    stringArray.addItem(
                                            (value instanceof NlsString)
                                                    ? ((NlsString) value).getValue()
                                                    : (String) value));
                    return Common.Value.newBuilder().setStrArray(stringArray).build();
                case DECIMAL:
                case FLOAT:
                case DOUBLE:
                    Common.DoubleArray.Builder doubleArray = Common.DoubleArray.newBuilder();
                    values.forEach(value -> doubleArray.addItem(((Number) value).doubleValue()));
                    return Common.Value.newBuilder().setF64Array(doubleArray).build();
                default:
                    throw new UnsupportedOperationException(
                            "can not convert sarg=" + sarg + " ir core array");
            }
        }
        switch (literal.getType().getSqlTypeName()) {
            case NULL:
                return Common.Value.newBuilder().setNone(Common.None.newBuilder().build()).build();
            case BOOLEAN:
                return Common.Value.newBuilder().setBoolean((Boolean) literal.getValue()).build();
            case INTEGER:
                return Common.Value.newBuilder()
                        .setI32(((Number) literal.getValue()).intValue())
                        .build();
            case BIGINT:
                return Common.Value.newBuilder()
                        .setI64(((Number) literal.getValue()).longValue())
                        .build();
            case CHAR:
                String valueStr =
                        (literal.getValue() instanceof NlsString)
                                ? ((NlsString) literal.getValue()).getValue()
                                : (String) literal.getValue();
                return Common.Value.newBuilder().setStr(valueStr).build();
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return Common.Value.newBuilder()
                        .setF64(((Number) literal.getValue()).doubleValue())
                        .build();
            default:
                throw new UnsupportedOperationException(
                        "literal type " + literal.getTypeName() + " is unsupported yet");
        }
    }

    public static final OuterExpression.Property protoProperty(GraphProperty property) {
        switch (property.getOpt()) {
            case ID:
                return OuterExpression.Property.newBuilder()
                        .setId(OuterExpression.IdKey.newBuilder().build())
                        .build();
            case LABEL:
                return OuterExpression.Property.newBuilder()
                        .setLabel(OuterExpression.LabelKey.newBuilder().build())
                        .build();
            case LEN:
                return OuterExpression.Property.newBuilder()
                        .setLen(OuterExpression.LengthKey.newBuilder().build())
                        .build();
            case ALL:
                return OuterExpression.Property.newBuilder()
                        .setAll(OuterExpression.AllKey.newBuilder().build())
                        .build();
            case KEY:
            default:
                return OuterExpression.Property.newBuilder()
                        .setKey(protoNameOrId(property.getKey()))
                        .build();
        }
    }

    public static final Common.NameOrId protoNameOrId(GraphNameOrId nameOrId) {
        switch (nameOrId.getOpt()) {
            case NAME:
                return Common.NameOrId.newBuilder().setName(nameOrId.getName()).build();
            case ID:
            default:
                return Common.NameOrId.newBuilder().setId(nameOrId.getId()).build();
        }
    }

    public static final OuterExpression.ExprOpr protoOperator(SqlOperator operator) {
        switch (operator.getKind()) {
            case PLUS:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.ADD)
                        .build();
            case MINUS:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.SUB)
                        .build();
            case TIMES:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.MUL)
                        .build();
            case DIVIDE:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.DIV)
                        .build();
            case MOD:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.MOD)
                        .build();
            case OTHER_FUNCTION:
                if (operator.getName().equals("POWER")) {
                    return OuterExpression.ExprOpr.newBuilder()
                            .setArith(OuterExpression.Arithmetic.EXP)
                            .build();
                } else if (operator.getName().equals("<<")) {
                    return OuterExpression.ExprOpr.newBuilder()
                            .setArith(OuterExpression.Arithmetic.BITLSHIFT)
                            .build();
                } else if (operator.getName().equals(">>")) {
                    return OuterExpression.ExprOpr.newBuilder()
                            .setArith(OuterExpression.Arithmetic.BITRSHIFT)
                            .build();
                }
            case EQUALS:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.EQ)
                        .build();
            case NOT_EQUALS:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.NE)
                        .build();
            case GREATER_THAN:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.GT)
                        .build();
            case GREATER_THAN_OR_EQUAL:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.GE)
                        .build();
            case LESS_THAN:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.LT)
                        .build();
            case LESS_THAN_OR_EQUAL:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.LE)
                        .build();
            case AND:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.AND)
                        .build();
            case OR:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.OR)
                        .build();
            case NOT:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.NOT)
                        .build();
            case IS_NULL:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.ISNULL)
                        .build();
            case SEARCH:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.WITHIN)
                        .build();
            case POSIX_REGEX_CASE_SENSITIVE:
                return OuterExpression.ExprOpr.newBuilder()
                        .setLogical(OuterExpression.Logical.REGEX)
                        .build();
            case BIT_AND:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.BITAND)
                        .build();
            case BIT_OR:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.BITOR)
                        .build();
            case BIT_XOR:
                return OuterExpression.ExprOpr.newBuilder()
                        .setArith(OuterExpression.Arithmetic.BITXOR)
                        .build();
            case OTHER:
                if (operator.getName().equals("IN")) {
                    return OuterExpression.ExprOpr.newBuilder()
                            .setLogical(OuterExpression.Logical.WITHIN)
                            .build();
                }
            default:
                throw new UnsupportedOperationException(
                        "operator type="
                                + operator.getKind()
                                + ", name="
                                + operator.getName()
                                + " is unsupported yet");
        }
    }

    public static final Common.DataType protoBasicDataType(RelDataType basicType) {
        // hack ways: convert interval type to int64 to avoid complexity
        if (basicType instanceof IntervalSqlType) return Common.DataType.INT64;
        if (basicType instanceof GraphLabelType) return Common.DataType.INT32;
        switch (basicType.getSqlTypeName()) {
            case NULL:
                return Common.DataType.NONE;
            case BOOLEAN:
                return Common.DataType.BOOLEAN;
            case INTEGER:
                return Common.DataType.INT32;
            case BIGINT:
                return Common.DataType.INT64;
            case CHAR:
                return Common.DataType.STRING;
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
                return Common.DataType.DOUBLE;
            case MULTISET:
            case ARRAY:
                RelDataType elementType = basicType.getComponentType();
                switch (elementType.getSqlTypeName()) {
                    case INTEGER:
                        return Common.DataType.INT32_ARRAY;
                    case BIGINT:
                        return Common.DataType.INT64_ARRAY;
                    case CHAR:
                        return Common.DataType.STRING_ARRAY;
                    case DECIMAL:
                    case FLOAT:
                    case DOUBLE:
                        return Common.DataType.DOUBLE_ARRAY;
                    default:
                        throw new UnsupportedOperationException(
                                "array of element type "
                                        + elementType.getSqlTypeName()
                                        + " is unsupported yet");
                }
            case DATE:
                return Common.DataType.DATE32;
            case TIME:
                return Common.DataType.TIME32;
            case TIMESTAMP:
                return Common.DataType.TIMESTAMP;
            default:
                throw new UnsupportedOperationException(
                        "basic type " + basicType.getSqlTypeName() + " is unsupported yet");
        }
    }

    public static final DataType.IrDataType protoIrDataType(
            RelDataType dataType, boolean isColumnId) {
        switch (dataType.getSqlTypeName()) {
            case ROW:
                if (dataType instanceof GraphSchemaType) {
                    DataType.GraphDataType.Builder builder = DataType.GraphDataType.newBuilder();
                    builder.setElementOpt(
                            protoElementOpt(((GraphSchemaType) dataType).getScanOpt()));
                    ((GraphSchemaType) dataType)
                            .getSchemaTypeAsList()
                            .forEach(
                                    k -> builder.addGraphDataType(protoElementType(k, isColumnId)));
                    return DataType.IrDataType.newBuilder().setGraphType(builder.build()).build();
                }
                throw new UnsupportedOperationException(
                        "convert row type "
                                + dataType.getSqlTypeName()
                                + " to IrDataType is unsupported yet");
            case MULTISET:
            case ARRAY:
            case MAP:
                SqlTypeName typeName =
                        (dataType != null && dataType.getComponentType() != null)
                                ? dataType.getComponentType().getSqlTypeName()
                                : null;
                List<SqlTypeName> basicTypes =
                        ImmutableList.of(
                                SqlTypeName.BOOLEAN,
                                SqlTypeName.INTEGER,
                                SqlTypeName.BIGINT,
                                SqlTypeName.CHAR,
                                SqlTypeName.DECIMAL,
                                SqlTypeName.FLOAT,
                                SqlTypeName.DOUBLE);
                if (typeName == null || !basicTypes.contains(typeName)) {
                    logger.warn(
                            "collection type with component type = ["
                                    + typeName
                                    + "] can not be converted to any ir core data type");
                    return DataType.IrDataType.newBuilder().build();
                }
            default:
                return DataType.IrDataType.newBuilder()
                        .setDataType(protoBasicDataType(dataType))
                        .build();
        }
    }

    public static final List<GraphAlgebra.MetaData> protoRowType(
            RelDataType rowType, boolean isColumnId) {
        switch (rowType.getSqlTypeName()) {
            case ROW:
                return rowType.getFieldList().stream()
                        .map(
                                k ->
                                        GraphAlgebra.MetaData.newBuilder()
                                                .setType(protoIrDataType(k.getType(), isColumnId))
                                                .setAlias(k.getIndex())
                                                .build())
                        .collect(Collectors.toList());
            default:
                throw new UnsupportedOperationException(
                        "convert type "
                                + rowType.getSqlTypeName()
                                + " to List<MetaData> is unsupported");
        }
    }

    public static final DataType.GraphDataType.GraphElementOpt protoElementOpt(
            GraphOpt.Source opt) {
        return DataType.GraphDataType.GraphElementOpt.valueOf(opt.name());
    }

    public static final DataType.GraphDataType.GraphElementType protoElementType(
            GraphSchemaType type, boolean isColumnId) {
        DataType.GraphDataType.GraphElementType.Builder builder =
                DataType.GraphDataType.GraphElementType.newBuilder()
                        .setLabel(protoElementLabel(type.getLabelType()));
        type.getFieldList()
                .forEach(
                        k -> {
                            DataType.GraphDataType.GraphElementTypeField.Builder fieldBuilder =
                                    DataType.GraphDataType.GraphElementTypeField.newBuilder();
                            // convert property name to id
                            if (isColumnId) {
                                fieldBuilder.setPropId(
                                        Common.NameOrId.newBuilder().setId(k.getIndex()).build());
                            } else {
                                fieldBuilder.setPropId(
                                        Common.NameOrId.newBuilder().setName(k.getName()).build());
                            }
                            fieldBuilder.setType(protoBasicDataType(k.getType()));
                            builder.addProps(fieldBuilder);
                        });
        return builder.build();
    }

    public static final DataType.GraphDataType.GraphElementLabel protoElementLabel(
            GraphLabelType labelType) {
        Preconditions.checkArgument(
                labelType.getLabelsEntry().size() == 1,
                "can not convert label=" + labelType + " to proto 'GraphElementLabel'");
        GraphLabelType.Entry entry = labelType.getSingleLabelEntry();
        DataType.GraphDataType.GraphElementLabel.Builder builder =
                DataType.GraphDataType.GraphElementLabel.newBuilder().setLabel(entry.getLabelId());
        if (entry.getSrcLabelId() != null) {
            builder.setSrcLabel(Int32Value.of(entry.getSrcLabelId()));
        }
        if (entry.getDstLabelId() != null) {
            builder.setDstLabel(Int32Value.of(entry.getDstLabelId()));
        }
        return builder.build();
    }

    public static final OuterExpression.Extract.Interval protoInterval(RexLiteral literal) {
        TimeUnit timeUnit;
        if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            timeUnit = literal.getValueAs(TimeUnit.class);
        } else if (literal.getType() instanceof IntervalSqlType) {
            timeUnit = literal.getType().getIntervalQualifier().getUnit();
        } else {
            throw new IllegalArgumentException("cannot get interval field from literal " + literal);
        }
        switch (timeUnit) {
            case YEAR:
                return OuterExpression.Extract.Interval.YEAR;
            case MONTH:
                return OuterExpression.Extract.Interval.MONTH;
            case DAY:
                return OuterExpression.Extract.Interval.DAY;
            case HOUR:
                return OuterExpression.Extract.Interval.HOUR;
            case MINUTE:
                return OuterExpression.Extract.Interval.MINUTE;
            case SECOND:
                return OuterExpression.Extract.Interval.SECOND;
            case MILLISECOND:
                return OuterExpression.Extract.Interval.MILLISECOND;
            default:
                throw new UnsupportedOperationException("unsupported interval type " + timeUnit);
        }
    }

    public static final Aggregate protoAggFn(GraphAggCall aggCall) {
        switch (aggCall.getAggFunction().getKind()) {
            case COUNT:
                return aggCall.isDistinct() ? Aggregate.COUNT_DISTINCT : Aggregate.COUNT;
            case COLLECT:
                return aggCall.isDistinct() ? Aggregate.TO_SET : Aggregate.TO_LIST;
            case SUM:
            case SUM0:
                return Aggregate.SUM;
            case AVG:
                return Aggregate.AVG;
            case MIN:
                return Aggregate.MIN;
            case MAX:
                return Aggregate.MAX;
            case FIRST_VALUE:
                return Aggregate.FIRST;
            default:
                throw new UnsupportedOperationException(
                        "aggregate opt "
                                + aggCall.getAggFunction().getKind()
                                + " is unsupported yet");
        }
    }

    public static final GraphAlgebra.OrderBy.OrderingPair.Order protoOrderOpt(
            RelFieldCollation.Direction direction) {
        switch (direction) {
            case ASCENDING:
                return GraphAlgebra.OrderBy.OrderingPair.Order.ASC;
            case DESCENDING:
                return GraphAlgebra.OrderBy.OrderingPair.Order.DESC;
            case CLUSTERED:
                return GraphAlgebra.OrderBy.OrderingPair.Order.SHUFFLE;
            default:
                throw new UnsupportedOperationException(
                        "direction " + direction + " in order is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.EdgeExpand.Direction protoExpandDirOpt(
            GraphOpt.Expand opt) {
        switch (opt) {
            case OUT:
                return GraphAlgebraPhysical.EdgeExpand.Direction.OUT;
            case IN:
                return GraphAlgebraPhysical.EdgeExpand.Direction.IN;
            case BOTH:
                return GraphAlgebraPhysical.EdgeExpand.Direction.BOTH;
            default:
                throw new UnsupportedOperationException(
                        "opt " + opt + " in expand is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.EdgeExpand.ExpandOpt protoExpandOpt(
            GraphOpt.PhysicalExpandOpt opt) {
        switch (opt) {
            case EDGE:
                return GraphAlgebraPhysical.EdgeExpand.ExpandOpt.EDGE;
            case VERTEX:
                return GraphAlgebraPhysical.EdgeExpand.ExpandOpt.VERTEX;
            case DEGREE:
                return GraphAlgebraPhysical.EdgeExpand.ExpandOpt.DEGREE;
            default:
                throw new UnsupportedOperationException(
                        "opt " + opt + " in expand is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.GetV.VOpt protoGetVOpt(GraphOpt.PhysicalGetVOpt opt) {
        switch (opt) {
            case START:
                return GraphAlgebraPhysical.GetV.VOpt.START;
            case END:
                return GraphAlgebraPhysical.GetV.VOpt.END;
            case OTHER:
                return GraphAlgebraPhysical.GetV.VOpt.OTHER;
            case BOTH:
                return GraphAlgebraPhysical.GetV.VOpt.BOTH;
            case ITSELF:
                return GraphAlgebraPhysical.GetV.VOpt.ITSELF;
            default:
                throw new UnsupportedOperationException(
                        "opt " + opt + " in getV is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.PathExpand.PathOpt protoPathOpt(
            GraphOpt.PathExpandPath opt) {
        switch (opt) {
            case ARBITRARY:
                return GraphAlgebraPhysical.PathExpand.PathOpt.ARBITRARY;
            case SIMPLE:
                return GraphAlgebraPhysical.PathExpand.PathOpt.SIMPLE;
            case TRAIL:
                return GraphAlgebraPhysical.PathExpand.PathOpt.TRAIL;
            case ANY_SHORTEST:
                return GraphAlgebraPhysical.PathExpand.PathOpt.ANY_SHORTEST;
            case ALL_SHORTEST:
                return GraphAlgebraPhysical.PathExpand.PathOpt.ALL_SHORTEST;
            default:
                throw new UnsupportedOperationException(
                        "opt " + opt + " in path is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.PathExpand.ResultOpt protoPathResultOpt(
            GraphOpt.PathExpandResult opt) {
        switch (opt) {
            case END_V:
                return GraphAlgebraPhysical.PathExpand.ResultOpt.END_V;
            case ALL_V:
                return GraphAlgebraPhysical.PathExpand.ResultOpt.ALL_V;
            case ALL_V_E:
                return GraphAlgebraPhysical.PathExpand.ResultOpt.ALL_V_E;
            default:
                throw new UnsupportedOperationException(
                        "result opt " + opt + " in path is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate protoAggOpt(
            GraphAggCall aggCall) {
        switch (aggCall.getAggFunction().kind) {
            case COUNT:
                return aggCall.isDistinct()
                        ? GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.COUNT_DISTINCT
                        : GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.COUNT;
            case COLLECT:
                return aggCall.isDistinct()
                        ? GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.TO_SET
                        : GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.TO_LIST;
            case SUM:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.SUM;
            case SUM0:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.SUM;
            case AVG:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.AVG;
            case MIN:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.MIN;
            case MAX:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.MAX;
            case FIRST_VALUE:
                return GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate.FIRST;
            default:
                throw new UnsupportedOperationException(
                        "aggregate opt " + aggCall.getAggFunction().kind + " is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.Join.JoinKind protoJoinKind(JoinRelType joinRelType) {
        switch (joinRelType) {
            case INNER:
                return GraphAlgebraPhysical.Join.JoinKind.INNER;
            case LEFT:
                return GraphAlgebraPhysical.Join.JoinKind.LEFT_OUTER;
            case RIGHT:
                return GraphAlgebraPhysical.Join.JoinKind.RIGHT_OUTER;
            case FULL:
                return GraphAlgebraPhysical.Join.JoinKind.FULL_OUTER;
            case SEMI:
                return GraphAlgebraPhysical.Join.JoinKind.SEMI;
            case ANTI:
                return GraphAlgebraPhysical.Join.JoinKind.ANTI;
            default:
                throw new UnsupportedOperationException(
                        "join type " + joinRelType + " is unsupported yet");
        }
    }

    public static final GraphAlgebraPhysical.Scan.ScanOpt protoScanOpt(GraphOpt.Source opt) {
        switch (opt) {
            case VERTEX:
                return GraphAlgebraPhysical.Scan.ScanOpt.VERTEX;
            case EDGE:
                return GraphAlgebraPhysical.Scan.ScanOpt.EDGE;
            default:
                throw new UnsupportedOperationException("scan opt " + opt + " is unsupported yet");
        }
    }

    public static Common.NameOrId asNameOrId(int id) {
        Common.NameOrId.Builder builder = Common.NameOrId.newBuilder();
        builder.setId(id);
        return builder.build();
    }

    public static com.google.protobuf.Int32Value asAliasId(int id) {
        return com.google.protobuf.Int32Value.of(id);
    }

    public static final List<GraphAlgebraPhysical.PhysicalOpr.MetaData> physicalProtoRowType(
            RelDataType rowType, boolean isColumnId) {
        switch (rowType.getSqlTypeName()) {
            case ROW:
                return rowType.getFieldList().stream()
                        .map(
                                k ->
                                        GraphAlgebraPhysical.PhysicalOpr.MetaData.newBuilder()
                                                .setType(
                                                        com.alibaba.graphscope.common.ir.runtime
                                                                .proto.Utils.protoIrDataType(
                                                                k.getType(), isColumnId))
                                                .setAlias(k.getIndex())
                                                .build())
                        .collect(Collectors.toList());
            default:
                throw new UnsupportedOperationException(
                        "convert type "
                                + rowType.getSqlTypeName()
                                + " to List<MetaData> is unsupported");
        }
    }

    public static GraphAlgebraPhysical.Repartition protoShuffleRepartition(int keyId) {
        GraphAlgebraPhysical.Repartition.Shuffle.Builder shuffleBuilder =
                GraphAlgebraPhysical.Repartition.Shuffle.newBuilder();
        if (keyId != AliasInference.DEFAULT_ID) {
            shuffleBuilder.setShuffleKey(asAliasId(keyId));
        }
        return GraphAlgebraPhysical.Repartition.newBuilder().setToAnother(shuffleBuilder).build();
    }

    public static Map<Integer, Set<GraphNameOrId>> extractTagColumnsFromRexNodes(
            List<RexNode> exprs) {
        return exprs.stream()
                .map(
                        expr ->
                                expr.accept(
                                        new RexVariableAliasCollector<Pair<Integer, GraphNameOrId>>(
                                                true,
                                                var -> {
                                                    if (var.getProperty() != null
                                                            && (GraphProperty.Opt.ALL.equals(
                                                                            var.getProperty()
                                                                                    .getOpt())
                                                                    || GraphProperty.Opt.KEY.equals(
                                                                            var.getProperty()
                                                                                    .getOpt()))) {
                                                        return Pair.with(
                                                                var.getAliasId(),
                                                                var.getProperty().getKey());

                                                    } else return Pair.with(null, null);
                                                })))
                .flatMap(List::stream)
                .filter(k -> k.getValue0() != null && k.getValue1() != null)
                .collect(
                        Collectors.groupingBy(
                                pair -> pair.getValue0(),
                                Collectors.mapping(pair -> pair.getValue1(), Collectors.toSet())));
    }

    // extract columns from relDataType, and return e.g., {name, age}
    public static Set<GraphNameOrId> extractColumnsFromRelDataType(
            RelDataType relDataType, boolean isColumnId) {
        List<RelDataTypeField> recordColumns = relDataType.getFieldList();
        Set<GraphNameOrId> columns = new HashSet<>();
        for (int i = 0; i < recordColumns.size(); ++i) {
            RelDataType recordColumnType = recordColumns.get(i).getType();
            // if current column is a graph schema type, we extract all the fields (i.e., property
            // types) from it
            if (recordColumnType instanceof GraphSchemaType) {
                List<RelDataTypeField> propertyTypes =
                        ((GraphSchemaType) recordColumnType).getFieldList();
                for (RelDataTypeField propertyType : propertyTypes) {
                    if (isColumnId) {
                        columns.add(new GraphNameOrId(propertyType.getIndex()));
                    } else {
                        columns.add(new GraphNameOrId(propertyType.getName()));
                    }
                }
            }
        }
        return columns;
    }

    // remove properties from columns by checking if the tags refers to edge type or path type
    public static void removeEdgeProperties(
            RelDataType inputDataType, Map<Integer, Set<GraphNameOrId>> tagColumns) {
        List<RelDataTypeField> fieldTypes = inputDataType.getFieldList();
        Set<Integer> tags = tagColumns.keySet();
        // first, process the *HEAD* separately since it is a special case
        if (tags.contains(AliasInference.DEFAULT_ID)) {
            RelDataTypeField headFieldType = fieldTypes.get(fieldTypes.size() - 1);
            if (headFieldType.getType() instanceof GraphSchemaType
                    && GraphOpt.Source.EDGE.equals(
                            ((GraphSchemaType) headFieldType.getType()).getScanOpt())) {
                tags.remove(AliasInference.DEFAULT_ID);
            } else if (headFieldType.getType() instanceof GraphPathType) {
                tags.remove(AliasInference.DEFAULT_ID);
            }
        }

        if (tags.isEmpty()) {
            return;
        }
        // then, process other tags by checking if they are of edge type or path type
        List<Integer> removeKeys = new ArrayList<>();
        for (RelDataTypeField fieldType : fieldTypes) {
            if (tags.contains(fieldType.getIndex())) {
                if (fieldType.getType() instanceof GraphSchemaType
                        && GraphOpt.Source.EDGE.equals(
                                ((GraphSchemaType) fieldType.getType()).getScanOpt())) {
                    removeKeys.add(fieldType.getIndex());
                } else if (fieldType.getType() instanceof GraphPathType) {
                    removeKeys.add(fieldType.getIndex());
                }
            }
        }
        tagColumns.keySet().removeAll(removeKeys);
    }

    public static final StoredProcedure.Query.Builder protoProcedure(
            RexNode procedure, RexToProtoConverter converter) {
        RexCall procedureCall = (RexCall) procedure;
        StoredProcedure.Query.Builder builder = StoredProcedure.Query.newBuilder();
        SqlOperator operator = procedureCall.getOperator();
        builder.setQueryName(Common.NameOrId.newBuilder().setName(operator.getName()).build());
        List<RexNode> operands = procedureCall.getOperands();
        for (int i = 0; i < operands.size(); ++i) {
            // param name is omitted
            StoredProcedure.Argument.Builder paramBuilder =
                    StoredProcedure.Argument.newBuilder().setParamInd(i);
            OuterExpression.ExprOpr protoValue = operands.get(i).accept(converter).getOperators(0);
            switch (protoValue.getItemCase()) {
                case VAR:
                    paramBuilder.setVar(protoValue.getVar());
                    break;
                case CONST:
                    paramBuilder.setConst(protoValue.getConst());
                    break;
                default:
                    throw new IllegalArgumentException(
                            "cannot set value=" + protoValue + " to any parameter in procedure");
            }
            builder.addArguments(paramBuilder);
        }
        return builder;
    }
}
