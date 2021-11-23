package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public enum OpTransformFactory implements Function<BaseOp, Pointer> {
    SCAN_FUSION_OP {
        @Override
        public Pointer apply(BaseOp baseOp) {
            ScanFusionOp op = (ScanFusionOp) baseOp;
            Optional<OpArg> scanOpt = op.getScanOpt();
            if (!scanOpt.isPresent()) {
                throw new IllegalArgumentException("scan opt is not present");
            }
            FfiScanOpt ffiScanOpt = (FfiScanOpt) scanOpt.get().getArg();
            Pointer scan = irCoreLib.initScanOperator(ffiScanOpt);
            Optional<OpArg> labels = op.getLabels();
            if (scan != null && labels.isPresent()) {
                List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().getArg();
                for (FfiNameOrId.ByValue label : ffiLabels) {
                    PlanFactory.checkResult(IrCoreLibrary.FunctionName.addScanTableName,
                            irCoreLib.addScanTableName(scan, label));
                }
            }
            Optional<OpArg> predicate = op.getPredicate();
            if (predicate.isPresent()) {
                String expr = (String) predicate.get().getArg();
                irCoreLib.setScanPredicate(scan, expr);
            }
            // todo: add predicates
            // todo: add limit
            Optional<OpArg> ids = op.getIds();
            if (ids.isPresent()) {
                Pointer pairs = irCoreLib.initKvEquivPairs();
                List<FfiConst.ByValue> ffiIds = (List<FfiConst.ByValue>) ids.get().getArg();
                scan = irCoreLib.initIdxscanOperator(scan);
                for (int i = 0; i < ffiIds.size(); ++i) {
                    if (i == 1) {
                        logger.error("{}", "indexing based query can only support one argument, ignore others");
                        break;
                    }
                    PlanFactory.checkResult(IrCoreLibrary.FunctionName.andKvEquivPair,
                            irCoreLib.andKvEquivPair(pairs, irCoreLib.asIdKey(), ffiIds.get(i)));
                    irCoreLib.addIdxscanKvEquivPairs(scan, pairs);
                }
            }
            return scan;
        }
    },
    SELECT_OP {
        @Override
        public Pointer apply(BaseOp baseOp) {
            SelectOp op = (SelectOp) baseOp;
            Optional<OpArg> predicate = op.getPredicate();
            if (!predicate.isPresent()) {
                throw new IllegalArgumentException("predicate is not present");
            }
            String expr = (String) predicate.get().getArg();
            Pointer select = irCoreLib.initSelectOperator();
            PlanFactory.checkResult(IrCoreLibrary.FunctionName.setSelectPredicate,
                    irCoreLib.setSelectPredicate(select, expr));
            return select;
        }
    },
    EXPAND_OP {
        @Override
        public Pointer apply(BaseOp baseOp) {
            ExpandOp op = (ExpandOp) baseOp;
            Optional<OpArg> direction = op.getDirection();
            if (!direction.isPresent()) {
                throw new IllegalArgumentException("direction opt is not present");
            }
            FfiDirection ffiDirection = (FfiDirection) direction.get().getArg();
            Pointer expand = irCoreLib.initExpandBase(ffiDirection);
            Optional<OpArg> labels = op.getLabels();
            if (labels.isPresent()) {
                List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().getArg();
                for (FfiNameOrId.ByValue label : ffiLabels) {
                    PlanFactory.checkResult(IrCoreLibrary.FunctionName.addExpandLabel,
                            irCoreLib.addExpandLabel(expand, label));
                }
            }
            // todo: add properties
            // todo: add predicates
            // todo: add limit
            Optional<OpArg> edgeOpt = op.getEdgeOpt();
            if (!edgeOpt.isPresent()) {
                throw new IllegalArgumentException("edge opt is not present");
            }
            Boolean isEdge = (Boolean) edgeOpt.get().getArg();
            expand = irCoreLib.initEdgexpdOperator(expand, isEdge);
            return expand;
        }
    };

    private static Logger logger = LoggerFactory.getLogger(OpTransformFactory.class);
    private static final IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
}
