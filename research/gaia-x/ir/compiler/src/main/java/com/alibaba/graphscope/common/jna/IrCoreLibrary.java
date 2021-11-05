package com.alibaba.graphscope.common.jna;

import com.alibaba.graphscope.common.jna.type.*;
import com.google.common.collect.ImmutableMap;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

public interface IrCoreLibrary extends Library {
    IrCoreLibrary INSTANCE = Native.load("ir_core", IrCoreLibrary.class, ImmutableMap.of(
            Library.OPTION_TYPE_MAPPER, IrTypeMapper.INSTANCE,
            Library.OPTION_FUNCTION_MAPPER, IrFunctionMapper.INSTANCE));

    Pointer initLogicalPlan();

    void debugPlan(Pointer plan);

    void destroyLogicalPlan(Pointer plan);

    FfiJobBuffer.ByValue buildPhysicalPlan(Pointer plan);

    Pointer initScanOperator(FfiScanOpt opt);

    ResultCode addScanTableName(Pointer scan, FfiNameOrId.ByValue tableName);

    ResultCode appendScanOperator(Pointer plan, Pointer scan, int parent, IntByReference oprIdx);

    Pointer initIdxscanOperator(Pointer scan);

    ResultCode appendIdxscanOperator(Pointer plan, Pointer idxScan, int parent, IntByReference oprIdx);

    Pointer initKvEquivPairs();

    ResultCode andKvEquivPair(Pointer pairs, FfiProperty.ByValue property, FfiConst.ByValue value);

    ResultCode addIdxscanKvEquivPairs(Pointer idxScan, Pointer pairs);

    Pointer initExpandBase(FfiDirection direction);

    ResultCode addExpandLabel(Pointer expand, FfiNameOrId.ByValue label);

    Pointer initEdgexpdOperator(Pointer expand, boolean isEdge);

    ResultCode appendEdgexpdOperator(Pointer plan, Pointer edgeXpd, int parent, IntByReference oprIdx);

    ResultCode setEdgexpdAlias(Pointer edgeXpd, FfiNameOrId.ByValue alias);

    Pointer initLimitOperator();

    ResultCode setLimitRange(Pointer limit, int lower, int upper);

    ResultCode appendLimitOperator(Pointer plan, Pointer limit, int parent, IntByReference oprIdx);

    Pointer initSelectOperator();

    ResultCode setSelectPredicate(Pointer select, String predicate);

    ResultCode appendSelectOperator(Pointer plan, Pointer select, int parent, IntByReference oprIdx);

    Pointer initOrderbyOperator();

    ResultCode addOrderbyPair(Pointer orderBy, FfiVariable.ByValue variable, FfiOrderOpt orderOpt);

    ResultCode setOrderbyLimit(Pointer orderBy, int lower, int upper);

    ResultCode appendOrderbyOperator(Pointer plan, Pointer orderBy, int parent, IntByReference oprIdx);

    Pointer initProjectOperator(boolean isAppend);

    ResultCode addProjectMapping(Pointer project, String expr, FfiNameOrId.ByValue alias, boolean isQueryGiven);

    ResultCode appendProjectOperator(Pointer plan, Pointer project, int parent, IntByReference oprIdx);

    FfiNameOrId.ByValue cstrAsNameOrId(String name);

    FfiConst.ByValue cstrAsConst(String value);

    FfiConst.ByValue int64AsConst(long value);

    FfiProperty.ByValue asLabelKey();

    FfiProperty.ByValue asIdKey();

    FfiProperty.ByValue asPropertyKey(FfiNameOrId.ByValue key);

    FfiVariable.ByValue asVarTagOnly(FfiNameOrId.ByValue tag);

    FfiVariable.ByValue asVarPropertyOnly(FfiProperty.ByValue property);

    FfiVariable.ByValue asVar(FfiNameOrId.ByValue tag, FfiProperty.ByValue property);

    FfiVariable.ByValue asNoneVar();

    void destroyJobBuffer(FfiJobBuffer value);

    enum FunctionName {
        initLogicalPlan,
        debugPlan,
        destroyLogicalPlan,
        buildPhysicalPlan,
        initScanOperator,
        addScanTableName,
        appendScanOperator,
        initIdxscanOperator,
        appendIdxscanOperator,
        initKvEquivPairs,
        andKvEquivPair,
        addIdxscanKvEquivPairs,
        initExpandBase,
        addExpandLabel,
        initEdgexpdOperator,
        appendEdgexpdOperator,
        setEdgexpdAlias,
        initLimitOperator,
        setLimitRange,
        appendLimitOperator,
        initSelectOperator,
        setSelectPredicate,
        appendSelectOperator,
        initOrderbyOperator,
        addOrderbyPair,
        setOrderbyLimit,
        appendOrderbyOperator,
        initProjectOperator,
        addProjectMapping,
        appendProjectOperator,
        cstrAsNameOrId,
        cstrAsConst,
        int64AsConst,
        asLabelKey,
        asIdKey,
        asPropertyKey,
        asVarPpt
    }
}
