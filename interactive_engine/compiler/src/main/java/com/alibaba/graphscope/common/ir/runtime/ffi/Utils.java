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

package com.alibaba.graphscope.common.ir.runtime.ffi;

import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.jna.type.*;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;

/**
 * convert {@code RexNode} to data types in ffi
 */
public abstract class Utils {
    public static final FfiScanOpt ffiScanOpt(GraphOpt.Source opt) {
        switch (opt) {
            case VERTEX:
                return FfiScanOpt.Entity;
            case EDGE:
            default:
                return FfiScanOpt.Relation;
        }
    }

    public static final FfiDirection ffiDirection(GraphOpt.Expand opt) {
        switch (opt) {
            case OUT:
                return FfiDirection.Out;
            case IN:
                return FfiDirection.In;
            case BOTH:
            default:
                return FfiDirection.Both;
        }
    }

    public static final FfiVOpt ffiVOpt(GraphOpt.GetV opt) {
        switch (opt) {
            case START:
                return FfiVOpt.Start;
            case END:
                return FfiVOpt.End;
            case OTHER:
                return FfiVOpt.Other;
            case BOTH:
            default:
                return FfiVOpt.Both;
        }
    }

    public static final FfiConst.ByValue ffiConst(RexLiteral literal) {
        FfiConst.ByValue ffiConst = new FfiConst.ByValue();
        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                ffiConst.dataType = FfiDataType.Boolean;
                ffiConst.bool = (Boolean) literal.getValue();
                break;
            case INTEGER:
                ffiConst.dataType = FfiDataType.I32;
                ffiConst.int32 = ((Number) literal.getValue()).intValue();
                break;
            case BIGINT:
                ffiConst.dataType = FfiDataType.I64;
                ffiConst.int64 = ((Number) literal.getValue()).longValue();
                break;
            case FLOAT:
            case DOUBLE:
                ffiConst.dataType = FfiDataType.F64;
                ffiConst.float64 = ((Number) literal.getValue()).doubleValue();
                break;
            case CHAR:
                ffiConst.dataType = FfiDataType.Str;
                ffiConst.cstr =
                        (literal.getValue() instanceof NlsString)
                                ? ((NlsString) literal.getValue()).getValue()
                                : (String) literal.getValue();
                break;
            default:
                throw new UnsupportedOperationException(
                        "convert type "
                                + literal.getType().getSqlTypeName()
                                + " to FfiConst is unsupported");
        }
        return ffiConst;
    }

    public static final PathOpt ffiPathOpt(GraphOpt.PathExpandPath opt) {
        switch (opt) {
            case ARBITRARY:
                return PathOpt.Arbitrary;
            case SIMPLE:
            default:
                return PathOpt.Simple;
        }
    }

    public static final ResultOpt ffiResultOpt(GraphOpt.PathExpandResult opt) {
        switch (opt) {
            case EndV:
                return ResultOpt.EndV;
            case AllV:
            default:
                return ResultOpt.AllV;
        }
    }

    public static final FfiAggOpt ffiAggOpt(GraphAggCall aggCall) {
        switch (aggCall.getAggFunction().kind) {
            case COUNT:
                return aggCall.isDistinct() ? FfiAggOpt.CountDistinct : FfiAggOpt.Count;
            case COLLECT:
                return aggCall.isDistinct() ? FfiAggOpt.ToSet : FfiAggOpt.ToList;
            case SUM:
                return FfiAggOpt.Sum;
            case AVG:
                return FfiAggOpt.Avg;
            case MIN:
                return FfiAggOpt.Min;
            case MAX:
                return FfiAggOpt.Max;
            default:
                throw new UnsupportedOperationException(
                        "aggregate opt " + aggCall.getAggFunction().kind + " is unsupported yet");
        }
    }

    public static final FfiOrderOpt ffiOrderOpt(RelFieldCollation.Direction direction) {
        switch (direction) {
            case ASCENDING:
                return FfiOrderOpt.Asc;
            case DESCENDING:
                return FfiOrderOpt.Desc;
            case CLUSTERED:
                return FfiOrderOpt.Shuffle;
            default:
                throw new UnsupportedOperationException(
                        "direction " + direction + " in order is unsupported yet");
        }
    }
}
