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
package com.compiler.demo.server.plan;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.compiler.demo.server.GlobalEngineConf;
import com.compiler.demo.server.idmaker.IdMaker;
import com.compiler.demo.server.plan.extractor.TagKeyExtractorFactory;
import com.compiler.demo.server.plan.strategy.BySubTaskStep;
import com.compiler.demo.server.plan.translator.builder.PlanConfig;
import com.google.protobuf.ByteString;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class PlanUtils {
    private static final Logger logger = LoggerFactory.getLogger(PlanUtils.class);
    public static final String DIRECTION_OTHER = "OTHER";

    public static List<Long> intIdsAsLongList(Object[] ids) {
        List<Long> longList = new ArrayList<>();
        for (Object id : ids) {
            if (id instanceof Long) {
                longList.add(((Long) id));
            } else if (id instanceof Integer) {
                longList.add(((Integer) id).longValue());
            } else {
                throw new RuntimeException("invalid id type " + id.getClass());
            }
        }
        return longList;
    }

    public static PegasusClient.JobConfig getDefaultConfig(long queryId) {
        try {
            String queryName = "demo_query_" + queryId;
            // read engine default config
            Map<String, Object> jobConfig = GlobalEngineConf.getDefaultSysConf();
            Graph.Variables variables = GlobalEngineConf.getGlobalVariables();
            if (variables != null) {
                // update config if set in global variables
                jobConfig.forEach((k, v) -> {
                    Optional value = variables.get(k);
                    if (value.isPresent()) {
                        jobConfig.put(k, value.get());
                    }
                });
            }
            long hosts = ((List) jobConfig.get("hosts")).size();
            List<Long> servers = new ArrayList<>();
            for (long i = 0; i < hosts; ++i) {
                servers.add(i);
            }
            return PegasusClient.JobConfig.newBuilder()
                    .setJobId(queryId)
                    .setJobName(queryName)
                    .setWorkers((Integer) jobConfig.get("workers"))
                    .addAllServers(servers)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Gremlin.SelectStep.Pop convertFrom(Pop gremlinPop) {
        return Gremlin.SelectStep.Pop.valueOf(gremlinPop.name().toUpperCase());
    }

    public static List<String> getSelectKeysList(Step step) {
        String field = "selectKeys";
        try {
            return (List<String>) FieldUtils.readField(step, field, true);
            // return getPrivateField(step.getClass(), field);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + " not exist in step " + step.getClass(), e);
        }
    }

    public static Traversal.Admin getKeyTraversal(Step step) {
        String field = "keyTraversal";
        try {
            return (Traversal.Admin) FieldUtils.readField(step, field, true);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + "not exist in step " + step.getClass(), e);
        }
    }

    public static Traversal.Admin getValueTraversal(Step step) {
        String field = "valueTraversal";
        try {
            return (Traversal.Admin) FieldUtils.readField(step, field, true);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + "not exist in step " + step.getClass(), e);
        }
    }

    public static Bindings getGlobalBindings(GremlinExecutor executor) {
        String field = "globalBindings";
        try {
            return (Bindings) FieldUtils.readField(executor, field, true);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + "not exist in step " + executor.getClass(), e);
        }
    }

    public static Gremlin.OrderByComparePair.Order convertFrom(Order gremlinOrder) {
        return Gremlin.OrderByComparePair.Order.valueOf(gremlinOrder.name().toUpperCase());
    }

    public static Gremlin.OrderByStep constructFrom(ComparatorHolder holder, Configuration conf) {
        Gremlin.OrderByStep.Builder builder = Gremlin.OrderByStep.newBuilder();
        holder.getComparators().forEach((k) -> {
            Order order = (Order) ((Pair) k).getValue1();
            Traversal.Admin orderByKey = (Traversal.Admin) ((Pair) k).getValue0();
            Gremlin.OrderByComparePair.Builder pairs = Gremlin.OrderByComparePair.newBuilder()
                    .setOrder(PlanUtils.convertFrom(order));
            Gremlin.TagKey tagKey = TagKeyExtractorFactory.OrderBY.extractFrom(orderByKey, false, conf);
            if (!isEmpty(tagKey)) pairs.setKey(tagKey);
            builder.addPairs(pairs);
        });
        return builder.build();
    }

    public static boolean isEmpty(Gremlin.TagKey tagKey) {
        return tagKey == null || isNotSet(tagKey.getTag()) && tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.ITEM_NOT_SET;
    }

    public static boolean isNotSet(Gremlin.StepTag tag) {
        return tag.getItemCase() == Gremlin.StepTag.ItemCase.ITEM_NOT_SET;
    }

    public static void setFinalStaticField(Class<?> className, String name, Object newValue) throws Exception {
        Field classField = FieldUtils.getField(className, name, true);
        Field modifiersField = FieldUtils.getField(Field.class, "modifiers", true);
        modifiersField.setInt(classField, classField.getModifiers() & ~Modifier.FINAL);
        classField.set(null, newValue);
    }

    public static void print(AbstractBuilder job) {
        printGremlinStep(job.getSource());
        job.getPlan().getPlan().forEach(k -> {
            printOpr(k);
        });
    }

    public static void printGremlinStep(ByteString data) {
        try {
            Gremlin.GremlinStep step = Gremlin.GremlinStep.parseFrom(data);
            logger.info("{}", step);
        } catch (Exception e) {
            logger.error("exception is {}", e);
        }
    }

    public static void printOpr(PegasusClient.OperatorDef op) {
        try {
            if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.ITERATE) {
                logger.info("REPEAT {} {}", op.getCh(), op.getIterate().getMaxIters());
                logger.info("[");
                op.getIterate().getBody().getPlanList().forEach(p -> printOpr(p));
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.SUBTASK) {
                logger.info("joiner {}", op.getCh());
                logger.info("[");
                op.getSubtask().getTask().getPlanList().forEach(p -> printOpr(p));
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.LIMIT) {
                logger.info("LIMIT {}", op.getCh());
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.ORDER) {
                logger.info("SORT {} {}", op.getCh(), op.getOrder().getLimit());
                logger.info("[");
                printGremlinStep(op.getOrder().getCompare());
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.GROUP) {
                logger.info("GROUP {}", op.getCh());
                logger.info("[");
                printGremlinStep(op.getGroup().getMap());
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.FOLD) {
                logger.info("COUNT {}", op.getCh());
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.UNION) {
                for (int i = 0; i < op.getUnion().getBranchesList().size(); ++i) {
                    logger.info("UNION-{}", i);
                    op.getUnion().getBranches(i).getPlanList().forEach(p -> printOpr(p));
                }
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.DEDUP) {
                logger.info("DEDUP {}", op.getCh());
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.MAP) {
                logger.info("MAP {}", op.getCh());
                logger.info("[");
                printGremlinStep(op.getMap().getResource());
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.FLAT_MAP) {
                logger.info("FLAT_MAP {}", op.getCh());
                logger.info("[");
                printGremlinStep(op.getFlatMap().getResource());
                logger.info("]");
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.FILTER) {
                logger.info("FILTER {}", op.getCh());
                logger.info("[");
                printGremlinStep(op.getFilter().getResource());
                logger.info("]");
            } else {
                logger.error("not support");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Traversal.Admin[] toArray(List<Traversal.Admin> traversals) {
        if (traversals == null || traversals.isEmpty()) return null;
        Traversal.Admin[] newTraversals = new Traversal.Admin[traversals.size()];
        traversals.toArray(newTraversals);
        return newTraversals;
    }

    public static TraversalRing getTraversalRing(List<Traversal.Admin> traversals, boolean defaultId) {
        List<Traversal.Admin> modulateBy = new ArrayList<>();
        if (traversals != null) {
            modulateBy.addAll(traversals);
        }
        if (modulateBy.isEmpty() && defaultId) {
            modulateBy.add(new TokenTraversal(T.id));
        }
        if (modulateBy.isEmpty()) {
            return new TraversalRing();
        } else {
            return new TraversalRing(toArray(modulateBy));
        }
    }

    /**
     * @param step SelectOneStep or SelectStep
     * @return mapping relationships between tag and by-traversal (can be null)
     */
    public static Map<String, Traversal.Admin> getSelectTraversalMap(Step step) {
        TraversalRing modulateBy;
        List<String> selectTags = new ArrayList<>();
        if (step instanceof SelectOneStep) {
            modulateBy = getTraversalRing(((SelectOneStep) step).getLocalChildren(), false);
            selectTags.addAll(((SelectOneStep) step).getScopeKeys());
        } else if (step instanceof SelectStep) {
            modulateBy = getTraversalRing(((SelectStep) step).getLocalChildren(), false);
            selectTags = PlanUtils.getSelectKeysList(step);
        } else {
            throw new UnsupportedOperationException("cannot support step other than SelectOneStep and SelectStep " + step.getClass());
        }
        Map<String, Traversal.Admin> tagTraversals = new HashMap<>();
        for (String tag : selectTags) {
            tagTraversals.put(tag, modulateBy.next());
        }
        return tagTraversals;
    }

    public static <T, R> Map.Entry<T, R> getFirstEntry(Map<T, R> mapObj) {
        return mapObj.entrySet().iterator().next();
    }

    public static Gremlin.GroupByStep constructFrom(Step groupByStep, Configuration conf) {
        Traversal.Admin keyTraversal;
        if (groupByStep instanceof GroupStep || groupByStep instanceof GroupCountStep) {
            keyTraversal = PlanUtils.getKeyTraversal(groupByStep);
        } else {
            throw new UnsupportedOperationException("cannot support traversal other than " + groupByStep.getClass());
        }
        Gremlin.GroupByStep.Builder builder = Gremlin.GroupByStep.newBuilder();
        Gremlin.TagKey tagKey = TagKeyExtractorFactory.GroupKeyBy.extractFrom(keyTraversal, false, conf);
        if (!isEmpty(tagKey)) builder.setKey(tagKey);
        builder.setAccum(getAccumKind(groupByStep));
        return builder.build();
    }

    public static Gremlin.GroupByStep.AccumKind getAccumKind(Step groupByStep) {
        Traversal.Admin valueTraversal;
        if (groupByStep instanceof GroupStep) {
            valueTraversal = PlanUtils.getValueTraversal(groupByStep);
            // default to list
            if (valueTraversal == null || valueTraversal.getSteps().isEmpty()
                    || valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof FoldStep
                    || valueTraversal.getSteps().size() == 2 && isIdentityTraversalMap(valueTraversal.getStartStep())
                    && valueTraversal.getEndStep() instanceof FoldStep) {
                return Gremlin.GroupByStep.AccumKind.TO_LIST;
            } else if (valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof CountGlobalStep) {
                return Gremlin.GroupByStep.AccumKind.CNT;
            } else {
                throw new UnsupportedOperationException("cannot support other value traversal " + valueTraversal);
            }
        } else if (groupByStep instanceof GroupCountStep) {
            return Gremlin.GroupByStep.AccumKind.CNT;
        } else {
            throw new UnsupportedOperationException("cannot support step other than group by " + groupByStep.getClass());
        }
    }

    public static boolean isIdentityTraversalMap(Step step) {
        if (step instanceof TraversalMapStep) {
            Traversal.Admin mapTraversal = (Traversal.Admin) ((TraversalMapStep) step).getLocalChildren().get(0);
            return mapTraversal != null && mapTraversal instanceof IdentityTraversal;
        } else {
            return false;
        }
    }

    public static Gremlin.SubTaskJoiner getByJoiner(BySubTaskStep.JoinerType type) {
        if (type == BySubTaskStep.JoinerType.GroupKeyBy || type == BySubTaskStep.JoinerType.OrderBy) {
            return Gremlin.SubTaskJoiner.newBuilder().setByJoiner(Gremlin.ByJoiner.newBuilder()).build();
        } else if (type == BySubTaskStep.JoinerType.GroupValueBy) {
            return Gremlin.SubTaskJoiner.newBuilder().setGroupValueJoiner(Gremlin.GroupValueJoiner.newBuilder()).build();
        } else {
            throw new UnsupportedOperationException("cannot support other by joiner type " + type);
        }
    }

    public static IdMaker getTagIdMaker(Configuration conf) {
        return (IdMaker) conf.getProperty(PlanConfig.TAG_ID_MAKER);
    }
}
