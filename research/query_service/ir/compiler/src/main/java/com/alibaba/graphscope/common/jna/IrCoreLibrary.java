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

package com.alibaba.graphscope.common.jna;

import com.alibaba.graphscope.common.jna.type.*;
import com.google.common.collect.ImmutableMap;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

public interface IrCoreLibrary extends Library {
    IrCoreLibrary INSTANCE =
            Native.load(
                    "ir_core",
                    IrCoreLibrary.class,
                    ImmutableMap.of(
                            Library.OPTION_TYPE_MAPPER, IrTypeMapper.INSTANCE,
                            Library.OPTION_FUNCTION_MAPPER, IrFunctionMapper.INSTANCE));

    Pointer initLogicalPlan();

    void writePlanToJson(Pointer plan, String jsonFile);

    void destroyLogicalPlan(Pointer plan);

    FfiData.ByValue buildPhysicalPlan(Pointer plan, int workers, int servers);

    Pointer initScanOperator(FfiScanOpt opt);

    FfiError.ByValue appendScanOperator(
            Pointer plan, Pointer scan, int parent, IntByReference oprIdx);

    // set primary index
    Pointer initIndexPredicate();

    FfiError.ByValue andEquivPredicate(
            Pointer predicate, FfiProperty.ByValue key, FfiConst.ByValue value);

    FfiError.ByValue orEquivPredicate(
            Pointer predicate, FfiProperty.ByValue key, FfiConst.ByValue value);

    FfiError.ByValue addScanIndexPredicate(Pointer scan, Pointer predicate);

    FfiError.ByValue setScanParams(Pointer scan, Pointer params);

    FfiError.ByValue setScanAlias(Pointer scan, FfiAlias.ByValue alias);

    Pointer initEdgexpdOperator(boolean isEdge, FfiDirection direction);

    FfiError.ByValue appendEdgexpdOperator(
            Pointer plan, Pointer edgeXpd, int parent, IntByReference oprIdx);

    FfiError.ByValue setEdgexpdParams(Pointer edgeXpd, Pointer params);

    FfiError.ByValue setEdgexpdAlias(Pointer edgeXpd, FfiAlias.ByValue alias);

    Pointer initLimitOperator();

    FfiError.ByValue setLimitRange(Pointer limit, int lower, int upper);

    FfiError.ByValue appendLimitOperator(
            Pointer plan, Pointer limit, int parent, IntByReference oprIdx);

    Pointer initSelectOperator();

    FfiError.ByValue setSelectPredicate(Pointer select, String predicate);

    FfiError.ByValue appendSelectOperator(
            Pointer plan, Pointer select, int parent, IntByReference oprIdx);

    Pointer initOrderbyOperator();

    FfiError.ByValue addOrderbyPair(
            Pointer orderBy, FfiVariable.ByValue variable, FfiOrderOpt orderOpt);

    FfiError.ByValue setOrderbyLimit(Pointer orderBy, int lower, int upper);

    FfiError.ByValue appendOrderbyOperator(
            Pointer plan, Pointer orderBy, int parent, IntByReference oprIdx);

    Pointer initProjectOperator(boolean isAppend);

    FfiError.ByValue addProjectExprAlias(Pointer project, String expr, FfiAlias.ByValue alias);

    FfiError.ByValue appendProjectOperator(
            Pointer plan, Pointer project, int parent, IntByReference oprIdx);

    /// To initialize an As operator
    Pointer initAsOperator();

    /// Set the alias of the entity to As
    FfiError.ByValue setAsAlias(Pointer as, FfiAlias.ByValue alias);

    /// Append an As operator to the logical plan
    FfiError.ByValue appendAsOperator(Pointer plan, Pointer as, int parent, IntByReference oprIdx);

    Pointer initGroupbyOperator();

    FfiError.ByValue addGroupbyKeyAlias(
            Pointer groupBy, FfiVariable.ByValue key, FfiAlias.ByValue alias);

    FfiError.ByValue addGroupbyAggFn(Pointer groupBy, FfiAggFn.ByValue aggFn);

    FfiError.ByValue appendGroupbyOperator(
            Pointer plan, Pointer groupBy, int parent, IntByReference oprIdx);

    Pointer initDedupOperator();

    FfiError.ByValue addDedupKey(Pointer dedup, FfiVariable.ByValue var);

    FfiError.ByValue appendDedupOperator(
            Pointer plan, Pointer dedup, int parent, IntByReference oprIdx);

    Pointer initSinkOperator();

    FfiError.ByValue addSinkColumn(Pointer sink, FfiNameOrId.ByValue column);

    FfiError.ByValue appendSinkOperator(
            Pointer plan, Pointer sink, int parent, IntByReference oprIdx);

    Pointer initGetvOperator(FfiVOpt vOpt);

    FfiError.ByValue setGetvAlias(Pointer getV, FfiAlias.ByValue alias);

    FfiError.ByValue appendGetvOperator(
            Pointer plan, Pointer getV, int parent, IntByReference oprIdx);

    FfiError.ByValue setGetvParams(Pointer getV, Pointer params);

    Pointer initApplyOperator(int subtaskRoot, FfiJoinKind joinKind);

    FfiError.ByValue setApplyAlias(Pointer apply, FfiAlias.ByValue alias);

    FfiError.ByValue appendApplyOperator(
            Pointer plan, Pointer apply, int parent, IntByReference oprIdx);

    Pointer initPathxpdOperator(Pointer expand, boolean isWholePath);

    FfiError.ByValue setPathxpdAlias(Pointer pathXpd, FfiAlias.ByValue alias);

    FfiError.ByValue setPathxpdHops(Pointer pathXpd, int lower, int upper);

    FfiError.ByValue appendPathxpdOperator(
            Pointer plan, Pointer pathXpd, int parent, IntByReference oprIdx);

    Pointer initUnionOperator();

    FfiError.ByValue addUnionParent(Pointer union, int subRootId);

    FfiError.ByValue appendUnionOperator(Pointer plan, Pointer union, IntByReference oprIdx);

    Pointer initPatternOperator();

    Pointer initPatternSentence(FfiJoinKind joinKind);

    FfiError.ByValue setSentenceStart(Pointer sentence, FfiNameOrId.ByValue tag);

    FfiError.ByValue setSentenceEnd(Pointer sentence, FfiNameOrId.ByValue tag);

    FfiError.ByValue addSentenceBinder(Pointer sentence, Pointer binder, FfiBinderOpt opt);

    FfiError.ByValue addPatternSentence(Pointer pattern, Pointer sentence);

    FfiError.ByValue appendPatternOperator(
            Pointer plan, Pointer pattern, int parent, IntByReference oprIdx);

    FfiNameOrId.ByValue noneNameOrId();

    FfiNameOrId.ByValue cstrAsNameOrId(String name);

    FfiConst.ByValue cstrAsConst(String value);

    FfiConst.ByValue int32AsConst(int value);

    FfiConst.ByValue int64AsConst(long value);

    FfiProperty.ByValue asNoneKey();

    FfiProperty.ByValue asLabelKey();

    FfiProperty.ByValue asIdKey();

    FfiProperty.ByValue asLenKey();

    FfiProperty.ByValue asPropertyKey(FfiNameOrId.ByValue key);

    FfiVariable.ByValue asVarTagOnly(FfiNameOrId.ByValue tag);

    FfiVariable.ByValue asVarPropertyOnly(FfiProperty.ByValue property);

    FfiVariable.ByValue asVar(FfiNameOrId.ByValue tag, FfiProperty.ByValue property);

    FfiVariable.ByValue asNoneVar();

    FfiAggFn.ByValue initAggFn(FfiAggOpt aggregate, FfiAlias.ByValue alias);

    void destroyFfiData(FfiData.ByValue value);

    FfiError.ByValue setSchema(String schemaJson);

    Pointer initQueryParams();

    FfiError.ByValue addParamsTable(Pointer params, FfiNameOrId.ByValue table);

    FfiError.ByValue addParamsColumn(Pointer params, FfiNameOrId.ByValue column);

    FfiError.ByValue setParamsRange(Pointer params, int lower, int upper);

    FfiError.ByValue setParamsPredicate(Pointer params, String predicate);

    FfiError.ByValue setParamsIsAllColumns(Pointer params);

    FfiKeyResult.ByValue getKeyName(int keyId, FfiKeyType keyType);
}
