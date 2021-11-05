package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.common.jna.type.ResultCode;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanFactory {
    private static final Logger logger = LoggerFactory.getLogger(PlanFactory.class);

    public static Pointer getPocPlan() {
        IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();

        Pointer ptrScan = irCoreLib.initScanOperator(FfiScanOpt.Vertex);
        checkResult(IrCoreLibrary.FunctionName.addScanTableName,
                irCoreLib.addScanTableName(ptrScan, irCoreLib.cstrAsNameOrId("person")));

        checkResult(IrCoreLibrary.FunctionName.appendScanOperator,
                irCoreLib.appendScanOperator(ptrPlan, ptrScan, oprIdx.getValue(), oprIdx));

        Pointer ptrSelect = irCoreLib.initSelectOperator();
        irCoreLib.setSelectPredicate(ptrSelect, "@.id == 1");
        checkResult(IrCoreLibrary.FunctionName.appendSelectOperator,
                irCoreLib.appendSelectOperator(ptrPlan, ptrSelect, oprIdx.getValue(), oprIdx));

        Pointer ptrExpand = irCoreLib.initExpandBase(FfiDirection.Out);
        checkResult(IrCoreLibrary.FunctionName.addExpandLabel,
                irCoreLib.addExpandLabel(ptrExpand, irCoreLib.cstrAsNameOrId("knows")));

        Pointer ptrEdgeXpd = irCoreLib.initEdgexpdOperator(ptrExpand, false);
        checkResult(IrCoreLibrary.FunctionName.appendEdgexpdOperator,
                irCoreLib.appendEdgexpdOperator(ptrPlan, ptrEdgeXpd, oprIdx.getValue(), oprIdx));

        Pointer ptrLimit = irCoreLib.initLimitOperator();
        irCoreLib.setLimitRange(ptrLimit, 10, 11);
        checkResult(IrCoreLibrary.FunctionName.appendLimitOperator,
                irCoreLib.appendLimitOperator(ptrPlan, ptrLimit, oprIdx.getValue(), oprIdx));

        return ptrPlan;
    }

    public static Pointer getCR2Plan() {
        long personId = 1L;
        long maxDate = 20090123L;

        IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();

        Pointer ptrScan = irCoreLib.initScanOperator(FfiScanOpt.Vertex);
        Pointer ptrIdxScan = irCoreLib.initIdxscanOperator(ptrScan);

        Pointer ptrPairs = irCoreLib.initKvEquivPairs();
        checkResult(IrCoreLibrary.FunctionName.andKvEquivPair,
                irCoreLib.andKvEquivPair(ptrPairs, irCoreLib.asIdKey(), irCoreLib.int64AsConst(personId)));

        checkResult(IrCoreLibrary.FunctionName.addIdxscanKvEquivPairs,
                irCoreLib.addIdxscanKvEquivPairs(ptrIdxScan, ptrPairs));

        checkResult(IrCoreLibrary.FunctionName.appendIdxscanOperator,
                irCoreLib.appendIdxscanOperator(ptrPlan, ptrIdxScan, oprIdx.getValue(), oprIdx));

        Pointer ptrOut = irCoreLib.initExpandBase(FfiDirection.Out);
        checkResult(IrCoreLibrary.FunctionName.addExpandLabel,
                irCoreLib.addExpandLabel(ptrOut, irCoreLib.cstrAsNameOrId("knows")));

        Pointer ptrOutXpd = irCoreLib.initEdgexpdOperator(ptrOut, false);
        checkResult(IrCoreLibrary.FunctionName.setEdgexpdAlias,
                irCoreLib.setEdgexpdAlias(ptrOutXpd, irCoreLib.cstrAsNameOrId("p")));

        //todo: add operator like getv to represent the properties the vertex need carry on

        checkResult(IrCoreLibrary.FunctionName.appendEdgexpdOperator,
                irCoreLib.appendEdgexpdOperator(ptrPlan, ptrOutXpd, oprIdx.getValue(), oprIdx));

        Pointer ptrIn = irCoreLib.initExpandBase(FfiDirection.In);
        checkResult(IrCoreLibrary.FunctionName.addExpandLabel,
                irCoreLib.addExpandLabel(ptrIn, irCoreLib.cstrAsNameOrId("hasCreator")));

        Pointer ptrInXpd = irCoreLib.initEdgexpdOperator(ptrIn, false);
        checkResult(IrCoreLibrary.FunctionName.setEdgexpdAlias,
                irCoreLib.setEdgexpdAlias(ptrInXpd, irCoreLib.cstrAsNameOrId("m")));

        //todo: add operator like getv to represent the properties the vertex need carry on

        checkResult(IrCoreLibrary.FunctionName.appendEdgexpdOperator,
                irCoreLib.appendEdgexpdOperator(ptrPlan, ptrInXpd, oprIdx.getValue(), oprIdx));

        Pointer ptrSelect = irCoreLib.initSelectOperator();
        checkResult(IrCoreLibrary.FunctionName.setSelectPredicate,
                irCoreLib.setSelectPredicate(ptrSelect, "@.creationDate <= " + maxDate));

        checkResult(IrCoreLibrary.FunctionName.appendSelectOperator,
                irCoreLib.appendSelectOperator(ptrPlan, ptrSelect, oprIdx.getValue(), oprIdx));

        Pointer ptrOrderBy = irCoreLib.initOrderbyOperator();
        checkResult(IrCoreLibrary.FunctionName.addOrderbyPair,
                irCoreLib.addOrderbyPair(ptrOrderBy,
                        irCoreLib.asVarPropertyOnly(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("creationDate"))),
                        FfiOrderOpt.Desc));

        checkResult(IrCoreLibrary.FunctionName.addOrderbyPair,
                irCoreLib.addOrderbyPair(ptrOrderBy,
                        irCoreLib.asVarPropertyOnly(irCoreLib.asPropertyKey(irCoreLib.cstrAsNameOrId("id"))),
                        FfiOrderOpt.Asc));
        checkResult(IrCoreLibrary.FunctionName.setOrderbyLimit,
                irCoreLib.setOrderbyLimit(ptrOrderBy, 20, 21));

        checkResult(IrCoreLibrary.FunctionName.appendOrderbyOperator,
                irCoreLib.appendOrderbyOperator(ptrPlan, ptrOrderBy, oprIdx.getValue(), oprIdx));

        Pointer ptrProject = irCoreLib.initProjectOperator(false);
        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@p.id", irCoreLib.cstrAsNameOrId("p_id"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@p.firstName", irCoreLib.cstrAsNameOrId("p_firstName"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@p.lastName", irCoreLib.cstrAsNameOrId("p_lastName"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@m.id", irCoreLib.cstrAsNameOrId("m_id"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@m.imageFile", irCoreLib.cstrAsNameOrId("m_imageFile"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@m.creationDate", irCoreLib.cstrAsNameOrId("m_creationDate"), false));

        checkResult(IrCoreLibrary.FunctionName.addProjectMapping,
                irCoreLib.addProjectMapping(ptrProject,
                        "@m.content", irCoreLib.cstrAsNameOrId("m_content"), false));

        checkResult(IrCoreLibrary.FunctionName.appendProjectOperator,
                irCoreLib.appendProjectOperator(ptrPlan, ptrProject, oprIdx.getValue(), oprIdx));

        return ptrPlan;
    }

    public static void checkResult(IrCoreLibrary.FunctionName methodName, ResultCode result) {
        if (result != ResultCode.Success) {
            throw new RuntimeException(methodName.name() + " return error " + result);
        }
    }
}
