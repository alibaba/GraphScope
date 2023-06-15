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

    FfiResult.ByValue printPlanAsJson(Pointer plan);

    void destroyLogicalPlan(Pointer plan);

    FfiData.ByValue buildPhysicalPlan(Pointer plan, int workers, int servers);

    Pointer initScanOperator(FfiScanOpt opt);

    FfiResult.ByValue appendScanOperator(
            Pointer plan, Pointer scan, int parent, IntByReference oprIdx);

    // set primary index
    Pointer initIndexPredicate();

    FfiResult.ByValue andEquivPredicate(
            Pointer predicate, FfiProperty.ByValue key, FfiConst.ByValue value);

    FfiResult.ByValue orEquivPredicate(
            Pointer predicate, FfiProperty.ByValue key, FfiConst.ByValue value);

    FfiResult.ByValue addScanIndexPredicate(Pointer scan, Pointer predicate);

    FfiResult.ByValue setScanParams(Pointer scan, Pointer params);

    FfiResult.ByValue setScanAlias(Pointer scan, FfiAlias.ByValue alias);

    Pointer initEdgexpdOperator(FfiExpandOpt expandOpt, FfiDirection direction);

    FfiResult.ByValue appendEdgexpdOperator(
            Pointer plan, Pointer edgeXpd, int parent, IntByReference oprIdx);

    FfiResult.ByValue setEdgexpdParams(Pointer edgeXpd, Pointer params);

    FfiResult.ByValue setEdgexpdAlias(Pointer edgeXpd, FfiAlias.ByValue alias);

    Pointer initLimitOperator();

    FfiResult.ByValue setLimitRange(Pointer limit, int lower, int upper);

    FfiResult.ByValue appendLimitOperator(
            Pointer plan, Pointer limit, int parent, IntByReference oprIdx);

    Pointer initSelectOperator();

    FfiResult.ByValue setSelectPredicate(Pointer select, String predicate);

    FfiResult.ByValue setSelectPredicatePb(Pointer select, FfiPbPointer.ByValue pbPointer);

    FfiResult.ByValue appendSelectOperator(
            Pointer plan, Pointer select, int parent, IntByReference oprIdx);

    Pointer initOrderbyOperator();

    FfiResult.ByValue addOrderbyPair(
            Pointer orderBy, FfiVariable.ByValue variable, FfiOrderOpt orderOpt);

    FfiResult.ByValue addOrderbyPairPb(
            Pointer orderBy, FfiPbPointer.ByValue pbPointer, FfiOrderOpt orderOpt);

    FfiResult.ByValue setOrderbyLimit(Pointer orderBy, int lower, int upper);

    FfiResult.ByValue appendOrderbyOperator(
            Pointer plan, Pointer orderBy, int parent, IntByReference oprIdx);

    Pointer initProjectOperator(boolean isAppend);

    FfiResult.ByValue addProjectExprAlias(Pointer project, String expr, FfiAlias.ByValue alias);

    FfiResult.ByValue addProjectExprPbAlias(
            Pointer project, FfiPbPointer.ByValue pbPointer, FfiAlias.ByValue alias);

    FfiResult.ByValue appendProjectOperator(
            Pointer plan, Pointer project, int parent, IntByReference oprIdx);

    /// To initialize an As operator
    Pointer initAsOperator();

    /// Set the alias of the entity to As
    FfiResult.ByValue setAsAlias(Pointer as, FfiAlias.ByValue alias);

    /// Append an As operator to the logical plan
    FfiResult.ByValue appendAsOperator(Pointer plan, Pointer as, int parent, IntByReference oprIdx);

    Pointer initGroupbyOperator();

    FfiResult.ByValue addGroupbyKeyAlias(
            Pointer groupBy, FfiVariable.ByValue key, FfiAlias.ByValue alias);

    FfiResult.ByValue addGroupbyAggFn(
            Pointer group, FfiVariable.ByValue aggVal, FfiAggOpt aggOpt, FfiAlias.ByValue alias);

    FfiResult.ByValue addGroupbyKeyPbAlias(
            Pointer groupBy, FfiPbPointer.ByValue pbPointer, FfiAlias.ByValue alias);

    FfiResult.ByValue addGroupbyAggFnPb(
            Pointer group,
            FfiPbPointer.ByValue pbPointer,
            FfiAggOpt aggOpt,
            FfiAlias.ByValue alias);

    FfiResult.ByValue appendGroupbyOperator(
            Pointer plan, Pointer groupBy, int parent, IntByReference oprIdx);

    Pointer initDedupOperator();

    FfiResult.ByValue addDedupKey(Pointer dedup, FfiVariable.ByValue var);

    FfiResult.ByValue addDedupKeyPb(Pointer dedup, FfiPbPointer.ByValue pbPointer);

    FfiResult.ByValue appendDedupOperator(
            Pointer plan, Pointer dedup, int parent, IntByReference oprIdx);

    Pointer initSinkOperator();

    FfiResult.ByValue addSinkColumn(Pointer sink, FfiNameOrId.ByValue column);

    FfiResult.ByValue appendSinkOperator(
            Pointer plan, Pointer sink, int parent, IntByReference oprIdx);

    Pointer initGetvOperator(FfiVOpt vOpt);

    FfiResult.ByValue setGetvAlias(Pointer getV, FfiAlias.ByValue alias);

    FfiResult.ByValue appendGetvOperator(
            Pointer plan, Pointer getV, int parent, IntByReference oprIdx);

    FfiResult.ByValue setGetvParams(Pointer getV, Pointer params);

    Pointer initApplyOperator(int subtaskRoot, FfiJoinKind joinKind);

    FfiResult.ByValue setApplyAlias(Pointer apply, FfiAlias.ByValue alias);

    FfiResult.ByValue appendApplyOperator(
            Pointer plan, Pointer apply, int parent, IntByReference oprIdx);

    Pointer initPathxpdOperator(Pointer expand, PathOpt pathOpt, ResultOpt resultOpt);

    Pointer initPathxpdOperatorWithExpandBase(
            Pointer expand, Pointer getV, PathOpt pathOpt, ResultOpt resultOpt);

    FfiResult.ByValue setPathxpdAlias(Pointer pathXpd, FfiAlias.ByValue alias);

    FfiResult.ByValue setPathxpdHops(Pointer pathXpd, int lower, int upper);

    FfiResult.ByValue setPathxpdCondition(Pointer pathXpd, String predicate);

    FfiResult.ByValue appendPathxpdOperator(
            Pointer plan, Pointer pathXpd, int parent, IntByReference oprIdx);

    Pointer initUnionOperator();

    FfiResult.ByValue addUnionParent(Pointer union, int subRootId);

    FfiResult.ByValue appendUnionOperator(Pointer plan, Pointer union, IntByReference oprIdx);

    Pointer initPatternOperator();

    Pointer initPatternSentence(FfiJoinKind joinKind);

    FfiResult.ByValue setSentenceStart(Pointer sentence, FfiNameOrId.ByValue tag);

    FfiResult.ByValue setSentenceEnd(Pointer sentence, FfiNameOrId.ByValue tag);

    FfiResult.ByValue addSentenceBinder(Pointer sentence, Pointer binder, FfiBinderOpt opt);

    FfiResult.ByValue addPatternSentence(Pointer pattern, Pointer sentence);

    FfiResult.ByValue appendPatternOperator(
            Pointer plan, Pointer pattern, int parent, IntByReference oprIdx);

    void destroyFfiData(FfiData.ByValue value);

    FfiResult.ByValue setSchema(String schemaJson);

    Pointer initQueryParams();

    FfiResult.ByValue addParamsTable(Pointer params, FfiNameOrId.ByValue table);

    FfiResult.ByValue addParamsColumn(Pointer params, FfiNameOrId.ByValue column);

    FfiResult.ByValue setParamsRange(Pointer params, int lower, int upper);

    FfiResult.ByValue setParamsPredicate(Pointer params, String predicate);

    FfiResult.ByValue setParamsPredicatePb(Pointer params, FfiPbPointer.ByValue exprPb);

    FfiResult.ByValue setParamsIsAllColumns(Pointer params);

    FfiResult.ByValue setParamsSampleRatio(Pointer params, double sampleRatio);

    FfiResult.ByValue getKeyName(int keyId, FfiKeyType keyType);

    FfiResult.ByValue addParamsExtra(Pointer params, String key, String value);

    Pointer initSinkGraphOperator(String graphName);

    FfiResult.ByValue setScanMeta(Pointer scan, FfiPbPointer.ByValue meta);

    FfiResult.ByValue setEdgexpdMeta(Pointer edgexpd, FfiPbPointer.ByValue meta);

    FfiResult.ByValue setGetvMeta(Pointer getV, FfiPbPointer.ByValue meta);

    FfiResult.ByValue addProjectMeta(Pointer project, FfiPbPointer.ByValue meta);

    FfiResult.ByValue addGroupbyKeyValueMeta(Pointer groupBy, FfiPbPointer.ByValue meta);

    FfiResult.ByValue addPatternMeta(Pointer pattern, FfiPbPointer.ByValue meta);

    FfiResult.ByValue setUnfoldMeta(Pointer unfold, FfiPbPointer.ByValue meta);
}
