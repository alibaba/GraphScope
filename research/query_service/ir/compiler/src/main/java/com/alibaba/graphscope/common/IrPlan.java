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
import com.alibaba.graphscope.common.exception.*;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.MatchSentence;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.intermediate.process.SinkArg;
import com.alibaba.graphscope.common.intermediate.process.SinkByColumns;
import com.alibaba.graphscope.common.intermediate.process.SinkGraph;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.utils.ClassUtils;
import com.alibaba.graphscope.gremlin.Utils;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;

import org.apache.commons.io.FileUtils;
import org.javatuples.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

// represent ir plan as a chain of operators
public class IrPlan implements Closeable {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static String PLAN_JSON_FILE = "plan.json";
    private Pointer ptrPlan;
    private IrMeta meta;

    // call libc to transform from InterOpBase to c structure
    private enum TransformFactory implements Function<InterOpBase, Pointer> {
        SCAN_FUSION_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                ScanFusionOp op = (ScanFusionOp) baseOp;
                Optional<OpArg> scanOpt = op.getScanOpt();
                if (!scanOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "scanOpt", "not present");
                }
                FfiScanOpt ffiScanOpt = (FfiScanOpt) scanOpt.get().applyArg();
                Pointer scan = irCoreLib.initScanOperator(ffiScanOpt);

                // set params
                Optional<QueryParams> paramsOpt = op.getParams();
                if (paramsOpt.isPresent()) {
                    FfiError e1 = irCoreLib.setScanParams(scan, createParams(paramsOpt.get()));
                    if (e1.code != ResultCode.Success) {
                        throw new InterOpIllegalArgException(
                                baseOp.getClass(), "params", "setScanParams returns " + e1.msg);
                    }
                }

                // set index predicate
                Optional<OpArg> ids = op.getIds();
                if (ids.isPresent()) {
                    Pointer idsPredicate = irCoreLib.initIndexPredicate();
                    List<FfiConst.ByValue> ffiIds = (List<FfiConst.ByValue>) ids.get().applyArg();
                    for (int i = 0; i < ffiIds.size(); ++i) {
                        FfiError e2 =
                                irCoreLib.orEquivPredicate(
                                        idsPredicate, irCoreLib.asIdKey(), ffiIds.get(i));
                        if (e2.code != ResultCode.Success) {
                            throw new InterOpIllegalArgException(
                                    baseOp.getClass(), "ids", "orEquivPredicate returns " + e2.msg);
                        }
                    }
                    if (!ffiIds.isEmpty()) {
                        irCoreLib.addScanIndexPredicate(scan, idsPredicate);
                    }
                }

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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "predicate", "not present");
                }
                String expr = (String) predicate.get().applyArg();
                Pointer select = irCoreLib.initSelectOperator();
                FfiError error = irCoreLib.setSelectPredicate(select, expr);
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(),
                            "predicate",
                            "setSelectPredicate returns " + error.msg);
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "direction", "not present");
                }
                FfiDirection ffiDirection = (FfiDirection) direction.get().applyArg();
                Optional<OpArg> edgeOpt = op.getIsEdge();
                if (!edgeOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "edgeOpt", "not present");
                }
                Boolean isEdge = (Boolean) edgeOpt.get().applyArg();
                Pointer expand = irCoreLib.initEdgexpdOperator(isEdge, ffiDirection);

                // set params
                Optional<QueryParams> paramsOpt = op.getParams();
                if (paramsOpt.isPresent()) {
                    FfiError e1 = irCoreLib.setEdgexpdParams(expand, createParams(paramsOpt.get()));
                    if (e1.code != ResultCode.Success) {
                        throw new InterOpIllegalArgException(
                                baseOp.getClass(), "params", "setEdgexpdParams returns " + e1.msg);
                    }
                }

                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent() && ClassUtils.equalClass(baseOp, ExpandOp.class)) {
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
                FfiError error =
                        irCoreLib.setLimitRange(
                                ptrLimit,
                                (Integer) lower.get().applyArg(),
                                (Integer) upper.get().applyArg());
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "lower+upper", "setLimitRange returns " + error.msg);
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "exprWithAlias", "not present");
                }
                List<Pair> exprWithAlias = (List<Pair>) exprOpt.get().applyArg();
                // append always and sink by parameters
                Pointer ptrProject = irCoreLib.initProjectOperator(true);
                exprWithAlias.forEach(
                        p -> {
                            String expr = (String) p.getValue0();
                            FfiAlias.ByValue alias = (FfiAlias.ByValue) p.getValue1();
                            FfiError error = irCoreLib.addProjectExprAlias(ptrProject, expr, alias);
                            if (error.code != ResultCode.Success) {
                                throw new InterOpIllegalArgException(
                                        baseOp.getClass(),
                                        "exprWithAlias",
                                        "append returns " + error.msg);
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "varWithOrder", "not present");
                }
                List<Pair> orderList = (List<Pair>) varWithOpt.get().applyArg();
                if (orderList.isEmpty()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "varWithOrder", "should not be empty");
                }
                Pointer ptrOrder = irCoreLib.initOrderbyOperator();
                orderList.forEach(
                        pair -> {
                            FfiVariable.ByValue var = (FfiVariable.ByValue) pair.getValue0();
                            FfiOrderOpt opt = (FfiOrderOpt) pair.getValue1();
                            irCoreLib.addOrderbyPair(ptrOrder, var, opt);
                        });
                // top k
                Optional<OpArg> lower = op.getLower();
                Optional<OpArg> upper = op.getUpper();
                if (lower.isPresent() && upper.isPresent()) {
                    irCoreLib.setOrderbyLimit(
                            ptrOrder,
                            (Integer) lower.get().applyArg(),
                            (Integer) upper.get().applyArg());
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "groupKeys", "not present");
                }
                Optional<OpArg> groupValuesOpt = op.getGroupByValues();
                if (!groupValuesOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "groupValues", "not present");
                }
                // groupKeys is empty -> count
                List<Pair> groupKeys = (List<Pair>) groupKeysOpt.get().applyArg();
                // set group key
                groupKeys.forEach(
                        p -> {
                            FfiVariable.ByValue key = (FfiVariable.ByValue) p.getValue0();
                            FfiAlias.ByValue alias = (FfiAlias.ByValue) p.getValue1();
                            irCoreLib.addGroupbyKeyAlias(ptrGroup, key, alias);
                        });
                List<ArgAggFn> groupValues = (List<ArgAggFn>) groupValuesOpt.get().applyArg();
                if (groupValues.isEmpty()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "groupValues", "not present");
                }
                // set group value
                groupValues.forEach(
                        p -> {
                            irCoreLib.addGroupbyAggFn(ptrGroup, ArgUtils.asFfiAggFn(p));
                        });
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(),
                            "groupKeys+groupValues",
                            "the query given alias is unsupported");
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "dedupKeys", "not present");
                }
                List<FfiVariable.ByValue> dedupKeys =
                        (List<FfiVariable.ByValue>) dedupKeysOpt.get().applyArg();
                if (dedupKeys.isEmpty()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "dedupKeys", "should not be empty if present");
                }
                dedupKeys.forEach(
                        k -> {
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
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "sinkArg", "not present");
                }
                SinkArg sinkArg = (SinkArg) argOpt.get().applyArg();
                Pointer ptrSink;
                if (sinkArg instanceof SinkByColumns) {
                    ptrSink = irCoreLib.initSinkOperator();
                    List<FfiNameOrId.ByValue> columns = ((SinkByColumns) sinkArg).getColumnNames();
                    if (columns.isEmpty()) {
                        throw new InterOpIllegalArgException(
                                baseOp.getClass(), "selected columns", "is empty");
                    }

                    columns.forEach(
                            column -> {
                                FfiError error = irCoreLib.addSinkColumn(ptrSink, column);
                                if (error.code != ResultCode.Success) {
                                    throw new InterOpIllegalArgException(
                                            baseOp.getClass(),
                                            "columns",
                                            "addSinkColumn returns " + error.msg);
                                }
                            });
                } else if (sinkArg instanceof SinkGraph) {
                    String graphName = ((SinkGraph) sinkArg).getConfig(SinkGraph.GRAPH_NAME);
                    ptrSink = irCoreLib.initSinkGraphOperator(graphName);
                } else {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "sink", "target is invalid");
                }
                return ptrSink;
            }
        },
        PATH_EXPAND_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                PathExpandOp pathOp = (PathExpandOp) baseOp;
                int lower = (Integer) pathOp.getLower().get().applyArg();
                int upper = (Integer) pathOp.getUpper().get().applyArg();

                Pointer expand = EXPAND_OP.apply(baseOp);
                // todo: make isWholePath configurable
                Pointer pathExpand = irCoreLib.initPathxpdOperator(expand, false);
                irCoreLib.setPathxpdHops(pathExpand, lower, upper);

                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setPathxpdAlias(pathExpand, alias);
                }
                return pathExpand;
            }
        },
        GETV_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                GetVOp getVOp = (GetVOp) baseOp;
                Optional<OpArg> vOpt = getVOp.getGetVOpt();
                if (!vOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "getVOpt", "not present");
                }
                FfiVOpt ffiVOpt = (FfiVOpt) vOpt.get().applyArg();
                Pointer ptrGetV = irCoreLib.initGetvOperator(ffiVOpt);

                // set params
                Optional<QueryParams> paramsOpt = getVOp.getParams();
                if (paramsOpt.isPresent()) {
                    FfiError e1 = irCoreLib.setGetvParams(ptrGetV, createParams(paramsOpt.get()));
                    if (e1.code != ResultCode.Success) {
                        throw new InterOpIllegalArgException(
                                baseOp.getClass(), "params", "setGetvParams returns " + e1.msg);
                    }
                }

                // set alias
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setGetvAlias(ptrGetV, alias);
                }
                return ptrGetV;
            }
        },
        APPLY_OP {
            @Override
            public Pointer apply(InterOpBase baseOp) {
                ApplyOp applyOp = (ApplyOp) baseOp;
                Optional<OpArg> subRootOpt = applyOp.getSubRootId();
                if (!subRootOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "subRootId", "not present");
                }
                Optional<OpArg> joinKindOpt = applyOp.getJoinKind();
                if (!joinKindOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "joinKind", "not present");
                }
                int subRootId = (Integer) subRootOpt.get().applyArg();
                FfiJoinKind joinKind = (FfiJoinKind) joinKindOpt.get().applyArg();
                Pointer ptrApply = irCoreLib.initApplyOperator(subRootId, joinKind);

                // set alias
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    FfiAlias.ByValue alias = (FfiAlias.ByValue) aliasOpt.get().applyArg();
                    irCoreLib.setApplyAlias(ptrApply, alias);
                }
                return ptrApply;
            }
        },
        UNION_OP {
            public Pointer apply(InterOpBase baseOp) {
                UnionOp unionOp = (UnionOp) baseOp;
                Optional<OpArg> parentIdsOpt = unionOp.getParentIdList();
                if (!parentIdsOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "parentIdList", "not present");
                }
                List<Integer> parentIds = (List<Integer>) parentIdsOpt.get().applyArg();
                if (parentIds.isEmpty()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "parentIdList", "is empty");
                }
                Pointer ptrUnion = irCoreLib.initUnionOperator();
                parentIds.forEach(
                        id -> {
                            irCoreLib.addUnionParent(ptrUnion, id);
                        });
                return ptrUnion;
            }
        },
        MATCH_OP {
            public Pointer apply(InterOpBase baseOp) {
                MatchOp matchOp = (MatchOp) baseOp;
                List<MatchSentence> sentences =
                        (List<MatchSentence>) matchOp.getSentences().get().applyArg();
                if (sentences.isEmpty()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "sentences", "is empty");
                }
                Pointer ptrMatch = irCoreLib.initPatternOperator();
                sentences.forEach(
                        s -> {
                            InterOpCollection ops = s.getBinders();
                            Pointer ptrSentence = irCoreLib.initPatternSentence(s.getJoinKind());
                            irCoreLib.setSentenceStart(ptrSentence, s.getStartTag().alias);
                            irCoreLib.setSentenceEnd(ptrSentence, s.getEndTag().alias);
                            ops.unmodifiableCollection()
                                    .forEach(
                                            o -> {
                                                Pointer binder;
                                                FfiBinderOpt opt;
                                                if (Utils.equalClass(o, ExpandOp.class)) {
                                                    binder = EXPAND_OP.apply(o);
                                                    opt = FfiBinderOpt.Edge;
                                                } else if (Utils.equalClass(
                                                        o, PathExpandOp.class)) {
                                                    binder = PATH_EXPAND_OP.apply(o);
                                                    opt = FfiBinderOpt.Path;
                                                } else if (Utils.equalClass(o, GetVOp.class)) {
                                                    binder = GETV_OP.apply(o);
                                                    opt = FfiBinderOpt.Vertex;
                                                } else if (Utils.equalClass(o, SelectOp.class)) {
                                                    binder = SELECT_OP.apply(o);
                                                    opt = FfiBinderOpt.Select;
                                                } else {
                                                    throw new InterOpIllegalArgException(
                                                            baseOp.getClass(),
                                                            "sentences",
                                                            "binder "
                                                                    + o.getClass()
                                                                    + " is unsupported yet");
                                                }
                                                irCoreLib.addSentenceBinder(
                                                        ptrSentence, binder, opt);
                                            });
                            irCoreLib.addPatternSentence(ptrMatch, ptrSentence);
                        });
                Optional<OpArg> aliasOpt = baseOp.getAlias();
                if (aliasOpt.isPresent()) {
                    throw new InterOpIllegalArgException(
                            baseOp.getClass(), "match", "the query given alias is unsupported");
                }
                return ptrMatch;
            }
        };

        public Pointer createParams(QueryParams params) {
            Pointer ptrParams = irCoreLib.initQueryParams();
            for (FfiNameOrId.ByValue table : params.getTables()) {
                FfiError error = irCoreLib.addParamsTable(ptrParams, table);
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            InterOpBase.class, "table", "addParamsTable returns " + error.msg);
                }
            }
            for (FfiNameOrId.ByValue column : params.getColumns()) {
                FfiError error = irCoreLib.addParamsColumn(ptrParams, column);
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            InterOpBase.class, "column", "addParamsColumn returns " + error.msg);
                }
            }
            Optional<String> predicateOpt = params.getPredicate();
            if (predicateOpt.isPresent()) {
                FfiError error = irCoreLib.setParamsPredicate(ptrParams, predicateOpt.get());
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            InterOpBase.class,
                            "predicate",
                            "setParamsPredicate returns " + error.msg);
                }
            }
            Optional<Pair<Integer, Integer>> rangeOpt = params.getRange();
            if (rangeOpt.isPresent()) {
                Pair<Integer, Integer> range = rangeOpt.get();
                FfiError error =
                        irCoreLib.setParamsRange(ptrParams, range.getValue0(), range.getValue1());
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            InterOpBase.class, "range", "setParamsRange returns " + error.msg);
                }
            }
            params.getExtraParams()
                    .forEach(
                            (k, v) -> {
                                FfiError error = irCoreLib.addParamsExtra(ptrParams, k, v);
                                if (error.code != ResultCode.Success) {
                                    throw new InterOpIllegalArgException(
                                            InterOpBase.class,
                                            "extraParams",
                                            "addParamsExtra returns " + error.msg);
                                }
                            });
            if (params.isAllColumns()) {
                FfiError error = irCoreLib.setParamsIsAllColumns(ptrParams);
                if (error.code != ResultCode.Success) {
                    throw new InterOpIllegalArgException(
                            InterOpBase.class,
                            "setIsAll",
                            "setParamsIsAllColumns returns " + error.msg);
                }
            }
            return ptrParams;
        }
    }

    public IrPlan(IrMeta meta, InterOpCollection opCollection) {
        this.meta = meta;
        irCoreLib.setSchema(meta.getSchema());
        this.ptrPlan = irCoreLib.initLogicalPlan();
        // add snapshot to QueryParams
        for (InterOpBase op : opCollection.unmodifiableCollection()) {
            QueryParams params = null;
            if (op instanceof ScanFusionOp && ((ScanFusionOp) op).getParams().isPresent()) {
                params = ((ScanFusionOp) op).getParams().get();
            } else if (op instanceof ExpandOp && ((ExpandOp) op).getParams().isPresent()) {
                params = ((ExpandOp) op).getParams().get();
            } else if (op instanceof GetVOp && ((GetVOp) op).getParams().isPresent()) {
                params = ((GetVOp) op).getParams().get();
            }
            if (params != null && meta.isAcquireSnapshot()) {
                params.addExtraParams(
                        QueryParams.SNAPSHOT_CONFIG_NAME, String.valueOf(meta.getSnapshotId()));
            }
        }
        appendInterOpCollection(-1, opCollection);
    }

    public byte[] toPhysicalBytes(Configs configs) throws BuildPhysicalException {
        if (ptrPlan == null) {
            throw new BuildPhysicalException("ptrPlan is NullPointer");
        }
        // hack way to notify shuffle
        int servers = PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length;
        int workers = PegasusConfig.PEGASUS_WORKER_NUM.get(configs);
        FfiData.ByValue buffer = irCoreLib.buildPhysicalPlan(ptrPlan, workers, servers);
        FfiError error = buffer.error;
        if (error.code != ResultCode.Success) {
            throw new BuildPhysicalException(
                    "call libc returns " + error.code.name() + ", msg is " + error.msg);
        }
        byte[] bytes = buffer.getBytes();
        buffer.close();
        return bytes;
    }

    public String getPlanAsJson() throws IOException {
        String json = "";
        if (ptrPlan != null) {
            File file = new File(PLAN_JSON_FILE);
            if (file.exists()) {
                file.delete();
            }
            irCoreLib.writePlanToJson(ptrPlan, PLAN_JSON_FILE);
            json = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            if (file.exists()) {
                file.delete();
            }
        }
        return json;
    }

    @Override
    public void close() {
        if (ptrPlan != null) {
            irCoreLib.destroyLogicalPlan(ptrPlan);
        }
    }

    // return id of the first operator, id of the last operator
    private Pair<Integer, Integer> appendInterOpCollection(
            int parentId, InterOpCollection opCollection) {
        int subTaskRootId = 0;
        // if opCollection is empty which means identity(), will return the id of the op before the
        // union
        int unionParentId = parentId;
        IntByReference oprId = new IntByReference(parentId);
        List<InterOpBase> opList = opCollection.unmodifiableCollection();
        for (int i = 0; i < opList.size(); ++i) {
            InterOpBase op = opList.get(i);
            oprId = appendInterOp(oprId.getValue(), op);
            if (i == 0) {
                subTaskRootId = oprId.getValue();
            }
            unionParentId = oprId.getValue();
        }
        return Pair.with(subTaskRootId, unionParentId);
    }

    // return id of the current operator
    private IntByReference appendInterOp(int parentId, InterOpBase base)
            throws InterOpIllegalArgException, InterOpUnsupportedException, AppendInterOpException {
        FfiError error;
        IntByReference oprId = new IntByReference(parentId);
        if (ClassUtils.equalClass(base, ScanFusionOp.class)) {
            Pointer ptrScan = TransformFactory.SCAN_FUSION_OP.apply(base);
            error = irCoreLib.appendScanOperator(ptrPlan, ptrScan, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, SelectOp.class)) {
            Pointer ptrSelect = TransformFactory.SELECT_OP.apply(base);
            error = irCoreLib.appendSelectOperator(ptrPlan, ptrSelect, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, ExpandOp.class)) {
            Pointer ptrExpand = TransformFactory.EXPAND_OP.apply(base);
            error = irCoreLib.appendEdgexpdOperator(ptrPlan, ptrExpand, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, LimitOp.class)) {
            Pointer ptrLimit = TransformFactory.LIMIT_OP.apply(base);
            error = irCoreLib.appendLimitOperator(ptrPlan, ptrLimit, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, ProjectOp.class)) {
            Pointer ptrProject = TransformFactory.PROJECT_OP.apply(base);
            error = irCoreLib.appendProjectOperator(ptrPlan, ptrProject, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, OrderOp.class)) {
            Pointer ptrOrder = TransformFactory.ORDER_OP.apply(base);
            error = irCoreLib.appendOrderbyOperator(ptrPlan, ptrOrder, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, GroupOp.class)) {
            Pointer ptrGroup = TransformFactory.GROUP_OP.apply(base);
            error = irCoreLib.appendGroupbyOperator(ptrPlan, ptrGroup, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, DedupOp.class)) {
            Pointer ptrDedup = TransformFactory.DEDUP_OP.apply(base);
            error = irCoreLib.appendDedupOperator(ptrPlan, ptrDedup, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, SinkOp.class)) {
            Pointer ptrSink = TransformFactory.SINK_OP.apply(base);
            error = irCoreLib.appendSinkOperator(ptrPlan, ptrSink, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, PathExpandOp.class)) {
            Pointer ptrPathXPd = TransformFactory.PATH_EXPAND_OP.apply(base);
            error = irCoreLib.appendPathxpdOperator(ptrPlan, ptrPathXPd, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, GetVOp.class)) {
            Pointer ptrGetV = TransformFactory.GETV_OP.apply(base);
            error = irCoreLib.appendGetvOperator(ptrPlan, ptrGetV, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, ApplyOp.class)) {
            ApplyOp applyOp = (ApplyOp) base;
            Optional<OpArg> subOps = applyOp.getSubOpCollection();
            if (!subOps.isPresent()) {
                throw new InterOpIllegalArgException(
                        base.getClass(), "subOpCollection", "is not present in apply");
            }
            InterOpCollection opCollection = (InterOpCollection) subOps.get().applyArg();
            Pair<Integer, Integer> oprIdPair = appendInterOpCollection(-1, opCollection);
            applyOp.setSubRootId(
                    new OpArg(Integer.valueOf(oprIdPair.getValue0()), Function.identity()));

            Pointer ptrApply = TransformFactory.APPLY_OP.apply(base);
            error = irCoreLib.appendApplyOperator(ptrPlan, ptrApply, oprId.getValue(), oprId);
        } else if (ClassUtils.equalClass(base, UnionOp.class)
                || ClassUtils.equalClass(base, SubGraphAsUnionOp.class)) {
            UnionOp unionOp = (UnionOp) base;
            Optional<OpArg> subOpsListOpt = unionOp.getSubOpCollectionList();
            if (!subOpsListOpt.isPresent()) {
                throw new InterOpIllegalArgException(
                        base.getClass(), "subOpCollectionList", "is not present in union");
            }
            List<InterOpCollection> subOpsList =
                    (List<InterOpCollection>) subOpsListOpt.get().applyArg();
            if (subOpsList.isEmpty()) {
                throw new InterOpIllegalArgException(
                        base.getClass(), "subOpCollectionList", "is empty in union");
            }
            List<Integer> unionParentIds = new ArrayList<>();
            for (InterOpCollection opCollection : subOpsList) {
                Pair<Integer, Integer> oprIdPair = appendInterOpCollection(parentId, opCollection);
                unionParentIds.add(oprIdPair.getValue1());
            }
            unionOp.setParentIdList(new OpArg(unionParentIds, Function.identity()));
            Pointer ptrUnion = TransformFactory.UNION_OP.apply(base);
            error = irCoreLib.appendUnionOperator(ptrPlan, ptrUnion, oprId);
        } else if (ClassUtils.equalClass(base, MatchOp.class)) {
            Pointer ptrMatch = TransformFactory.MATCH_OP.apply(base);
            error = irCoreLib.appendPatternOperator(ptrPlan, ptrMatch, oprId.getValue(), oprId);
        } else {
            throw new InterOpUnsupportedException(base.getClass(), "unimplemented yet");
        }
        if (error != null && error.code != ResultCode.Success) {
            throw new AppendInterOpException(
                    base.getClass(), error.code.name() + ", msg is " + error.msg);
        }
        // add alias after the op if necessary
        return setPostAlias(oprId.getValue(), base);
    }

    private IntByReference setPostAlias(int parentId, InterOpBase base) {
        IntByReference oprId = new IntByReference(parentId);
        if (isPostAliasOp(base) && base.getAlias().isPresent()) {
            FfiAlias.ByValue ffiAlias = (FfiAlias.ByValue) base.getAlias().get().applyArg();
            Pointer ptrAs = irCoreLib.initAsOperator();
            FfiError error = irCoreLib.setAsAlias(ptrAs, ffiAlias);
            if (error != null && error.code != ResultCode.Success) {
                throw new AppendInterOpException(base.getClass(), error.msg);
            }
            FfiError appendOp = irCoreLib.appendAsOperator(ptrPlan, ptrAs, parentId, oprId);
            if (appendOp != null && appendOp.code != ResultCode.Success) {
                throw new AppendInterOpException(base.getClass(), appendOp.msg);
            }
        }
        return oprId;
    }

    private boolean isPostAliasOp(InterOpBase base) {
        return base instanceof SelectOp
                || base instanceof LimitOp
                || base instanceof OrderOp
                || base instanceof DedupOp
                || base instanceof UnionOp;
    }
}
