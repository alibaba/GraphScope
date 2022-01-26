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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.exception.AppendInterOpException;
import com.alibaba.graphscope.common.exception.BuildPhysicalException;
import com.alibaba.graphscope.common.exception.InterOpIllegalArgException;
import com.alibaba.graphscope.common.exception.InterOpUnsupportedException;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.intermediate.process.SinkArg;
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
import java.util.*;
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
                FfiScanOpt ffiScanOpt = (FfiScanOpt) scanOpt.get().applyArg();
                Pointer scan = irCoreLib.initScanOperator(ffiScanOpt);
                Optional<OpArg> labels = op.getLabels();
                if (labels.isPresent()) {
                    List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().applyArg();
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
                    String expr = (String) predicate.get().applyArg();
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
                    List<FfiConst.ByValue> ffiIds = (List<FfiConst.ByValue>) ids.get().applyArg();
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
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setScanAlias(scan, alias);
                }
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
                String expr = (String) predicate.get().applyArg();
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
                FfiDirection ffiDirection = (FfiDirection) direction.get().applyArg();
                Optional<OpArg> edgeOpt = op.getIsEdge();
                if (!edgeOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "edgeOpt", "not present");
                }
                Boolean isEdge = (Boolean) edgeOpt.get().applyArg();
                Pointer expand = irCoreLib.initEdgexpdOperator(isEdge, ffiDirection);
                Optional<OpArg> labels = op.getLabels();
                if (labels.isPresent()) {
                    List<FfiNameOrId.ByValue> ffiLabels = (List<FfiNameOrId.ByValue>) labels.get().applyArg();
                    for (FfiNameOrId.ByValue label : ffiLabels) {
                        ResultCode resultCode = irCoreLib.addEdgexpdLabel(expand, label);
                        if (resultCode != ResultCode.Success) {
                            throw new InterOpIllegalArgException(baseOp.getClass(),
                                    "labels", "addEdgexpdLabel returns " + resultCode.name());
                        }
                    }
                }
                // todo: add properties
                // todo: add predicates
                // todo: add limit
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setEdgexpdAlias(expand, alias);
                }
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
                        (Integer) lower.get().applyArg(), (Integer) upper.get().applyArg());
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
                Optional<OpArg> exprOpt = op.getExprWithAlias();
                if (!exprOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "exprWithAlias", "not present");
                }
                List<Pair> exprWithAlias = (List<Pair>) exprOpt.get().applyArg();
                // append always and sink by parameters
                Pointer ptrProject = irCoreLib.initProjectOperator(true);
                exprWithAlias.forEach(p -> {
                    String expr = (String) p.getValue0();
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) p.getValue1();
                    ResultCode resultCode = irCoreLib.addProjectExprAlias(ptrProject, expr, alias);
                    if (resultCode != ResultCode.Success) {
                        throw new InterOpIllegalArgException(baseOp.getClass(), "exprWithAlias", "append returns " + resultCode.name());
                    }
                });
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
                List<Pair> orderList = (List<Pair>) varWithOpt.get().applyArg();
                if (orderList.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "varWithOrder", "should not be empty");
                }
                Pointer ptrOrder = irCoreLib.initOrderbyOperator();
                orderList.forEach(pair -> {
                    FfiVariable.ByValue var = (FfiVariable.ByValue) pair.getValue0();
                    FfiOrderOpt opt = (FfiOrderOpt) pair.getValue1();
                    irCoreLib.addOrderbyPair(ptrOrder, var, opt);
                });
                // top k
                Optional<OpArg> lower = op.getLower();
                Optional<OpArg> upper = op.getUpper();
                if (lower.isPresent() && upper.isPresent()) {
                    irCoreLib.setOrderbyLimit(ptrOrder, (Integer) lower.get().applyArg(), (Integer) upper.get().applyArg());
                }
                return ptrOrder;
            }
        },
        GROUP_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                Pointer ptrGroup = irCoreLib.initGroupbyOperator();
                GroupOp op = (GroupOp) baseOp;
                Optional<OpArg> groupKeysOpt = op.getGroupByKeys();
                if (!groupKeysOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "groupKeys", "not present");
                }
                Optional<OpArg> groupValuesOpt = op.getGroupByValues();
                if (!groupValuesOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "groupValues", "not present");
                }
                // groupKeys is empty -> count
                List<Pair> groupKeys = (List<Pair>) groupKeysOpt.get().applyArg();
                // set group key
                groupKeys.forEach(p -> {
                    FfiVariable.ByValue key = (FfiVariable.ByValue) p.getValue0();
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) p.getValue1();
                    irCoreLib.addGroupbyKeyAlias(ptrGroup, key, alias);
                });
                List<ArgAggFn> groupValues = (List<ArgAggFn>) groupValuesOpt.get().applyArg();
                if (groupValues.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "groupValues", "not present");
                }
                // set group value
                groupValues.forEach(p -> {
                    irCoreLib.addGroupbyAggFn(ptrGroup, ArgUtils.asFfiAggFn(p));
                });
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "groupKeys+groupValues", "the query given alias is unsupported");
                }
                return ptrGroup;
            }
        },
        DEDUP_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                Pointer ptrDedup = irCoreLib.initDedupOperator();
                DedupOp op = (DedupOp) baseOp;
                Optional<OpArg> dedupKeysOpt = op.getDedupKeys();
                if (!dedupKeysOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "dedupKeys", "not present");
                }
                List<FfiVariable.ByValue> dedupKeys = (List<FfiVariable.ByValue>) dedupKeysOpt.get().applyArg();
                if (dedupKeys.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "dedupKeys", "should not be empty if present");
                }
                dedupKeys.forEach(k -> {
                    irCoreLib.addDedupKey(ptrDedup, k);
                });
                return ptrDedup;
            }
        },
        SINK_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                SinkOp sinkOp = (SinkOp) baseOp;
                Optional<OpArg> argOpt = sinkOp.getSinkArg();
                if (!argOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "sinkArg", "not present");
                }
                SinkArg sinkArg = (SinkArg) argOpt.get().applyArg();
                List<FfiNameOrId.ByValue> columns = sinkArg.getColumnNames();
                if (columns.isEmpty()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "selected columns", "is empty");
                }
                Pointer ptrSink = irCoreLib.initSinkOperator();
                columns.forEach(column -> {
                    ResultCode resultCode = irCoreLib.addSinkColumn(ptrSink, column);
                    if (resultCode != ResultCode.Success) {
                        throw new InterOpIllegalArgException(baseOp.getClass(), "columns", "addSinkColumn returns " + resultCode.name());
                    }
                });
                return ptrSink;
            }
        },
        GETV_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                GetVOp getVOp = (GetVOp) baseOp;
                Optional<OpArg> vOpt = getVOp.getGetVOpt();
                if (!vOpt.isPresent()) {
                    throw new InterOpIllegalArgException(baseOp.getClass(), "getVOpt", "not present");
                }
                FfiVOpt ffiVOpt = (FfiVOpt) vOpt.get().applyArg();
                Pointer ptrGetV = irCoreLib.initGetvOperator(ffiVOpt);

                // set alias
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setGetvAlias(ptrGetV, alias);
                }
                return ptrGetV;
            }
        }
    }

    public IrPlan() {
        this.ptrPlan = irCoreLib.initLogicalPlan(true);
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
        } else if (base instanceof GroupOp) {
            Pointer ptrGroup = TransformFactory.GROUP_OP.apply(base);
            resultCode = irCoreLib.appendGroupbyOperator(ptrPlan, ptrGroup, oprIdx.getValue(), oprIdx);
        } else if (base instanceof DedupOp) {
            Pointer ptrDedup = TransformFactory.DEDUP_OP.apply(base);
            resultCode = irCoreLib.appendDedupOperator(ptrPlan, ptrDedup, oprIdx.getValue(), oprIdx);
        } else if (base instanceof SinkOp) {
            Pointer ptrSink = TransformFactory.SINK_OP.apply(base);
            resultCode = irCoreLib.appendSinkOperator(ptrPlan, ptrSink, oprIdx.getValue(), oprIdx);
        } else if (base instanceof GetVOp) {
            Pointer ptrGetV = TransformFactory.GETV_OP.apply(base);
            resultCode = irCoreLib.appendGetvOperator(ptrPlan, ptrGetV, oprIdx.getValue(), oprIdx);
        } else {
            throw new InterOpUnsupportedException(base.getClass(), "unimplemented yet");
        }
        if (resultCode != null && resultCode != ResultCode.Success) {
            throw new AppendInterOpException(base.getClass(), resultCode);
        }
        // add alias after the op if necessary
        setPostAlias(base);
    }

    private void setPostAlias(InterOpBase base) {
        if (!(base instanceof ScanFusionOp || base instanceof ExpandOp
                || base instanceof ProjectOp || base instanceof GroupOp || base instanceof GetVOp)
                && base.getAlias().isPresent()) {
            FfiAlias.ByValue ffiAlias = (FfiAlias.ByValue) base.getAlias().get().applyArg();
            Pointer ptrAs = irCoreLib.initAsOperator();
            ResultCode asResult = irCoreLib.setAsAlias(ptrAs, ffiAlias);
            if (asResult != null && asResult != ResultCode.Success) {
                throw new AppendInterOpException(base.getClass(), asResult);
            }
            ResultCode appendResult = irCoreLib.appendAsOperator(ptrPlan, ptrAs, oprIdx.getValue(), oprIdx);
            if (appendResult != null && appendResult != ResultCode.Success) {
                throw new AppendInterOpException(base.getClass(), appendResult);
            }
        }
    }

    public byte[] toPhysicalBytes(Configs configs) throws BuildPhysicalException {
        if (ptrPlan == null) {
            throw new BuildPhysicalException("ptrPlan is NullPointer");
        }
        // hack way to notify shuffle
        int servers = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
        int workers = PegasusConfig.PEGASUS_WORKER_NUM.get(configs);
        FfiJobBuffer.ByValue buffer = irCoreLib.buildPhysicalPlan(ptrPlan, workers, servers);
        if (buffer.len == 0) {
            throw new BuildPhysicalException("call libc returns " + ResultCode.BuildJobError.name());
        }
        byte[] bytes = buffer.getBytes();
        buffer.close();
        return bytes;
    }
}
