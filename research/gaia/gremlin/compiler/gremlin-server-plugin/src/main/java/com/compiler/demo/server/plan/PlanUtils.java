/**
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
import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.service.proto.PegasusClient;
import com.compiler.demo.server.plan.extractor.TagKeyExtractorFactory;
import com.compiler.demo.server.plan.strategy.BySubTaskStep;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlanUtils {
    private static final Logger logger = LoggerFactory.getLogger(PlanUtils.class);

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

    public static PegasusClient.JobConfig getDefaultConfig() {
        // todo: incremental job id
        long queryId = 1;
        String queryName = "demo_query_" + queryId;
        return PegasusClient.JobConfig.newBuilder().setJobId(queryId).setJobName(queryName).setWorkers(2).build();
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

    public static Gremlin.OrderByComparePair.Order convertFrom(Order gremlinOrder) {
        return Gremlin.OrderByComparePair.Order.valueOf(gremlinOrder.name().toUpperCase());
    }

    public static Gremlin.OrderByStep constructFrom(ComparatorHolder holder) {
        Gremlin.OrderByStep.Builder builder = Gremlin.OrderByStep.newBuilder();
        holder.getComparators().forEach((k) -> {
            Order order = (Order) ((Pair) k).getValue1();
            Traversal.Admin orderByKey = (Traversal.Admin) ((Pair) k).getValue0();
            Gremlin.OrderByComparePair.Builder pairs = Gremlin.OrderByComparePair.newBuilder()
                    .setOrder(PlanUtils.convertFrom(order));
            Gremlin.TagKey tagKey = TagKeyExtractorFactory.OrderBY.extractFrom(orderByKey);
            if (!isEmpty(tagKey)) pairs.setKey(tagKey);
            builder.addPairs(pairs);
        });
        return builder.build();
    }

    public static boolean isEmpty(Gremlin.TagKey tagKey) {
        return tagKey == null || tagKey.getTag().isEmpty() && tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.ITEM_NOT_SET;
    }

    public static void setFinalStaticField(Class<?> className, String name, Object newValue) throws Exception {
        Field classField = FieldUtils.getField(className, name, true);
        Field modifiersField = FieldUtils.getField(Field.class, "modifiers", true);
        modifiersField.setInt(classField, classField.getModifiers() & ~Modifier.FINAL);
        classField.set(null, newValue);
    }

    public static void print(JobBuilder job) {
        printGremlinStep(job.getSource());
        job.getPlan().getPlan().forEach(k -> {
            printOpr(k);
        });
    }

    public static void printGremlinStep(ByteString data) {
        try {
            Gremlin.GremlinStep step = Gremlin.GremlinStep.parseFrom(data);
            logger.info(step.toString());
        } catch (Exception e) {
            logger.error("exception is {}", e);
        }
    }

    public static void printOpr(PegasusClient.OperatorDef op) {
        try {
            if (op.getOpKind() == PegasusClient.OpKind.REPEAT) {
                PegasusClient.NestedTask task = op.getNestedTask(0);
                logger.info(task.getRepeatCond().toString());
                task.getPlanList().forEach(p -> printOpr(p));
            } else if (op.getOpKind() == PegasusClient.OpKind.SUBTASK) {
                PegasusClient.NestedTask task = op.getNestedTask(0);
                logger.info(task.getJoiner().toString());
                task.getPlanList().forEach(p -> printOpr(p));
            } else if (op.getOpKind() == PegasusClient.OpKind.LIMIT) {
                logger.info("LIMIT");
            } else if (op.getOpKind() == PegasusClient.OpKind.SORT) {
                PegasusClient.SortBy sortBy = PegasusClient.SortBy.parseFrom(op.getResource());
                logger.info("SORT {}", sortBy.getLimit());
                logger.info("[");
                printGremlinStep(sortBy.getCmp());
                logger.info("]");
            } else if (op.getOpKind() == PegasusClient.OpKind.GROUP) {
                PegasusClient.GroupBy groupBy = PegasusClient.GroupBy.parseFrom(op.getResource());
                logger.info("GROUP {}", groupBy.getAccum());
                logger.info("[");
                printGremlinStep(groupBy.getGetKey());
                logger.info("]");
            } else if (op.getOpKind() == PegasusClient.OpKind.COUNT) {
                logger.info("COUNT");
            } else if (op.getOpKind() == PegasusClient.OpKind.UNION) {
                logger.info("UNION {}", op.getNestedTaskList());
            } else if (op.getOpKind() == PegasusClient.OpKind.DEDUP) {
                logger.info("DEDUP");
            } else {
                logger.info("[");
                logger.info(op.getOpKind().toString());
                printGremlinStep(op.getResource());
                logger.info(op.getCh().toString());
                logger.info("]");
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
            modulateBy.add(new ElementValueTraversal(T.id.getAccessor()));
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

    public static Gremlin.GroupByStep constructFrom(Step groupByStep) {
        Traversal.Admin keyTraversal;
        if (groupByStep instanceof GroupStep || groupByStep instanceof GroupCountStep) {
            keyTraversal = PlanUtils.getKeyTraversal(groupByStep);
        } else {
            throw new UnsupportedOperationException("cannot support traversal other than " + groupByStep.getClass());
        }
        Gremlin.GroupByStep.Builder builder = Gremlin.GroupByStep.newBuilder();
        Gremlin.TagKey tagKey = TagKeyExtractorFactory.GroupBy.extractFrom(keyTraversal);
        if (!isEmpty(tagKey)) builder.setKey(tagKey);
        return builder.build();
    }

    public static PegasusClient.AccumKind getAccumKind(Step groupByStep) {
        Traversal.Admin valueTraversal;
        if (groupByStep instanceof GroupStep) {
            valueTraversal = PlanUtils.getValueTraversal(groupByStep);
            // default to list
            if (valueTraversal == null || valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof FoldStep
                    || valueTraversal.getSteps().size() == 2 && isIdentityTraversalMap(valueTraversal.getStartStep())
                    && valueTraversal.getEndStep() instanceof FoldStep) {
                return PegasusClient.AccumKind.TO_LIST;
            } else if (valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof CountGlobalStep) {
                return PegasusClient.AccumKind.CNT;
            } else {
                throw new UnsupportedOperationException("cannot support other value traversal " + valueTraversal);
            }
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
        if (type == BySubTaskStep.JoinerType.GroupBy || type == BySubTaskStep.JoinerType.OrderBy) {
            return Gremlin.SubTaskJoiner.newBuilder().setByJoiner(Gremlin.ByJoiner.newBuilder()).build();
        } else {
            throw new UnsupportedOperationException("cannot support other by joiner type " + type);
        }
    }
}
