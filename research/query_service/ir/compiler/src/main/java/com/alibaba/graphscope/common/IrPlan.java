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

package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.exception.AppendInterOpException;
import com.alibaba.graphscope.common.exception.BuildPhysicalException;
import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;
import com.alibaba.graphscope.common.exception.InterOpUnsupportedException;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.apache.commons.io.FileUtils;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

// represent ir plan as a chain of operators
public class IrPlan implements Closeable {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static Logger logger = LoggerFactory.getLogger(IrPlan.class);
    private static String PLAN_JSON_FILE = "plan.json";
    private Pointer ptrPlan;
    private IntByReference oprIdx;

    // call libc to transform from InterOpBase to c structure
    private enum TransformFactory implements Function<InterOpBase, Pointer> {
        SCAN_FUSION_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                ScanFusionOp op = (ScanFusionOp) baseOp;
                Optional<OpArg> scanOpt = op.getScanOpt();
                if (!scanOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "scanOpt", "not present");
                }
                FfiScanOpt ffiScanOpt = (FfiScanOpt) scanOpt.get().getArg();
                Pointer scan = irCoreLib.initScanOperator(ffiScanOpt);
                Optional<OpArg> labels = op.getLabels();
                if (labels.isPresent()) {
                    List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().getArg();
                    for (FfiNameOrId.ByValue label : ffiLabels) {
                        ResultCode resultCode = irCoreLib.addScanTableName(scan, label);
                        if (resultCode != ResultCode.Success) {
                            throw new InterOpIllegalArgException(baseOp.getClass(),
                                    "labels", "addScanTableName returns " + resultCode.name());
                        }
                    }
                }
                Optional<OpArg> predicate = op.getPredicate();
                if (predicate.isPresent()) {
                    String expr = (String) predicate.get().getArg();
                    ResultCode resultCode = irCoreLib.setScanPredicate(scan, expr);
                    if (resultCode != ResultCode.Success) {
                        throw new InterOpIllegalArgException(baseOp.getClass(),
                                "predicate", "setScanPredicate returns " + resultCode.name());
                    }
                }
                // set index predicate
                Optional<OpArg> ids = op.getIds();
                if (ids.isPresent()) {
                    Pointer idsPredicate = irCoreLib.initIndexPredicate();
                    List<FfiConst.ByValue> ffiIds = (List<FfiConst.ByValue>) ids.get().getArg();
                    for (int i = 0; i < ffiIds.size(); ++i) {
                        ResultCode resultCode = irCoreLib.orEquivPredicate(idsPredicate, irCoreLib.asIdKey(), ffiIds.get(i));
                        if (resultCode != ResultCode.Success) {
                            throw new InterOpIllegalArgException(baseOp.getClass(),
                                    "ids", "orEquivPredicate returns " + resultCode.name());
                        }
                    }
                    if (!ffiIds.isEmpty()) {
                        irCoreLib.addScanIndexPredicate(scan, idsPredicate);
                    }
                }
                // todo: add other predicates
                // todo: add limit
                return scan;
            }
        },
        SELECT_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                SelectOp op = (SelectOp) baseOp;
                Optional<OpArg> predicate = op.getPredicate();
                if (!predicate.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "predicate", "not present");
                }
                String expr = (String) predicate.get().getArg();
                Pointer select = irCoreLib.initSelectOperator();
                ResultCode resultCode = irCoreLib.setSelectPredicate(select, expr);
                if (resultCode != ResultCode.Success) {
                    throw new InterOpIllegalArgException(baseOp.getClass(),
                            "predicate", "setSelectPredicate returns " + resultCode.name());
                }
                return select;
            }
        },
        EXPAND_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                ExpandOp op = (ExpandOp) baseOp;
                Optional<OpArg> direction = op.getDirection();
                if (!direction.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "direction", "not present");
                }
                FfiDirection ffiDirection = (FfiDirection) direction.get().getArg();
                Pointer expand = irCoreLib.initExpandBase(ffiDirection);
                Optional<OpArg> labels = op.getLabels();
                if (labels.isPresent()) {
                    List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().getArg();
                    for (FfiNameOrId.ByValue label : ffiLabels) {
                        ResultCode resultCode = irCoreLib.addExpandLabel(expand, label);
                        if (resultCode != ResultCode.Success) {
                            throw new InterOpIllegalArgException(baseOp.getClass(),
                                    "labels", "addExpandLabel returns " + resultCode.name());
                        }
                    }
                }
                // todo: add properties
                // todo: add predicates
                // todo: add limit
                Optional<OpArg> edgeOpt = op.getIsEdge();
                if (!edgeOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "edgeOpt", "not present");
                }
                Boolean isEdge = (Boolean) edgeOpt.get().getArg();
                expand = irCoreLib.initEdgexpdOperator(expand, isEdge);
                return expand;
            }
        },
        LIMIT_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                LimitOp op = (LimitOp) baseOp;
                Optional<OpArg> lower = op.getLower();
                if (!lower.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "lower", "not present");
                }
                Optional<OpArg> upper = op.getUpper();
                if (!upper.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "upper", "not present");
                }
                Pointer ptrLimit = irCoreLib.initLimitOperator();
                ResultCode resultCode = irCoreLib.setLimitRange(ptrLimit,
                        (Integer) lower.get().getArg(), (Integer) upper.get().getArg());
                if (resultCode != ResultCode.Success) {
                    throw new InterOpIllegalArgException(baseOp.getClass(),
                            "lower+upper", "setLimitRange returns " + resultCode.name());
                }
                return ptrLimit;
            }
        },
        PROJECT_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                ProjectOp op = (ProjectOp) baseOp;
                Optional<OpArg> exprWithAlias = op.getProjectExprWithAlias();
                if (!exprWithAlias.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "exprWithAlias", "not present");
                }
                List<Pair> exprList = (List<Pair>) exprWithAlias.get().getArg();
                if (exprList.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "exprWithAlias", "should not be empty");
                }
                // todo: make append configurable
                Pointer ptrProject = irCoreLib.initProjectOperator(false);
                exprList.forEach(pair -> {
                    String expr = (String) pair.getValue0();
                    FfiNameOrId.ByValue alias = (FfiNameOrId.ByValue) pair.getValue1();
                    // aliases generated by compiler all start with ~
                    boolean isQueryGiven = !ArgUtils.isHiddenStr(alias.name);
                    irCoreLib.addProjectExprAlias(ptrProject, expr, alias, isQueryGiven);
                });
                if (baseOp.getAlias().isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "alias", "unimplemented yet");
                }
                return ptrProject;
            }
        },
        ORDER_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                OrderOp op = (OrderOp) baseOp;
                Optional<OpArg> varWithOpt = op.getOrderVarWithOrder();
                if (!varWithOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "varWithOrder", "not present");
                }
                List<Pair> orderList = (List<Pair>) varWithOpt.get().getArg();
                if (orderList.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "varWithOrder", "should not be empty");
                }
                Pointer ptrOrder = irCoreLib.initOrderbyOperator();
                orderList.forEach(pair -> {
                    FfiVariable.ByValue var = (FfiVariable.ByValue) pair.getValue0();
                    FfiOrderOpt opt = (FfiOrderOpt) pair.getValue1();
                    irCoreLib.addOrderbyPair(ptrOrder, var, opt);
                });
                if (baseOp.getAlias().isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "alias", "unimplemented yet");
                }
                return ptrOrder;
            }
        }
    }

    public IrPlan() {
        this.ptrPlan = irCoreLib.initLogicalPlan();
        this.oprIdx = new IntByReference(0);
    }

    @Override
    public void close() {
        if (ptrPlan != null) {
            irCoreLib.destroyLogicalPlan(ptrPlan);
        }
    }

    public String getPlanAsJson() throws IOException {
        String json = "";
        if (ptrPlan != null) {
            File file = new File(PLAN_JSON_FILE);
            if (file.exists()) {
                file.delete();
            }
            irCoreLib.write_plan_to_json(ptrPlan, PLAN_JSON_FILE);
            json = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            if (file.exists()) {
                file.delete();
            }
        }
        return json;
    }

    public void appendInterOp(InterOpBase base) throws
            InterOpIllegalArgException, InterOpUnsupportedException, AppendInterOpException {
        ResultCode resultCode;
        if (base instanceof ScanFusionOp) {
            Pointer ptrScan = TransformFactory.SCAN_FUSION_OP.apply(base);
            resultCode = irCoreLib.appendScanOperator(ptrPlan, ptrScan, oprIdx.getValue(), oprIdx);
        } else if (base instanceof SelectOp) {
            Pointer ptrSelect = TransformFactory.SELECT_OP.apply(base);
            resultCode = irCoreLib.appendSelectOperator(ptrPlan, ptrSelect, oprIdx.getValue(), oprIdx);
        } else if (base instanceof ExpandOp) {
            Pointer ptrExpand = TransformFactory.EXPAND_OP.apply(base);
            resultCode = irCoreLib.appendEdgexpdOperator(ptrPlan, ptrExpand, oprIdx.getValue(), oprIdx);
        } else if (base instanceof LimitOp) {
            Pointer ptrLimit = TransformFactory.LIMIT_OP.apply(base);
            resultCode = irCoreLib.appendLimitOperator(ptrPlan, ptrLimit, oprIdx.getValue(), oprIdx);
        } else if (base instanceof ProjectOp) {
            Pointer ptrProject = TransformFactory.PROJECT_OP.apply(base);
            resultCode = irCoreLib.appendProjectOperator(ptrPlan, ptrProject, oprIdx.getValue(), oprIdx);
        } else if (base instanceof OrderOp) {
            Pointer ptrOrder = TransformFactory.ORDER_OP.apply(base);
            resultCode = irCoreLib.appendOrderbyOperator(ptrPlan, ptrOrder, oprIdx.getValue(), oprIdx);
        } else {
            throw new InterOpUnsupportedException(base.getClass(), "unimplemented yet");
        }
        if (resultCode != null && resultCode != ResultCode.Success) {
            throw new AppendInterOpException(base.getClass(), resultCode);
        }
    }

    public byte[] toPhysicalBytes() throws BuildPhysicalException {
        if (ptrPlan == null) {
            throw new BuildPhysicalException("ptrPlan is NullPointer");
        }
        FfiJobBuffer.ByValue buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        if (buffer.len == 0) {
            throw new BuildPhysicalException("call libc returns " + ResultCode.BuildJobError.name());
        }
        byte[] bytes = buffer.getBytes();
        buffer.close();
        return bytes;
    }
}
