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
package com.alibaba.graphscope.gaia.plan;

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.pegasus.builder.AbstractBuilder;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import com.alibaba.graphscope.gaia.plan.strategy.BySubTaskStep;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class PlanUtils {
    private static final Logger logger = LoggerFactory.getLogger(PlanUtils.class);
    public static final String DIRECTION_OTHER = "OTHER";

    public static PegasusClient.JobConfig getDefaultConfig(long queryId, GaiaConfig config) {
        try {
            String queryName = "gaia_query_" + queryId;
            return PegasusClient.JobConfig.newBuilder()
                    .setJobId(queryId)
                    .setJobName(queryName)
                    .setWorkers(config.getPegasusWorkerNum())
                    .addAllServers(config.getPegasusServers())
                    .setTimeLimit(config.getPegasusTimeout())
                    .setBatchSize(config.getPegasusBatchSize())
                    .setOutputCapacity(config.getPegasusOutputCapacity())
                    .setMemoryLimit(config.getPegasusMemoryLimit())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Gremlin.SelectStep.Pop convertFrom(Pop gremlinPop) {
        return Gremlin.SelectStep.Pop.valueOf(gremlinPop.name().toUpperCase());
    }

    public static Gremlin.OrderByComparePair.Order convertFrom(Order gremlinOrder) {
        return Gremlin.OrderByComparePair.Order.valueOf(gremlinOrder.name().toUpperCase());
    }

    public static Gremlin.PropKeys convertFrom(ToFetchProperties toFetchProperties) {
        Gremlin.PropKeys.Builder keysBuilder = Gremlin.PropKeys.newBuilder();
        if (toFetchProperties.isAll()) {
            keysBuilder.setIsAll(true);
        } else if (toFetchProperties.getProperties() != null && !toFetchProperties.getProperties().isEmpty()) {
            List<String> properties = toFetchProperties.getProperties();
            if (StringUtils.isNumeric(properties.get(0))) {
                keysBuilder.addAllPropKeys(properties.stream()
                        .map(k -> Common.PropertyKey.newBuilder().setNameId(Integer.valueOf(k)).build())
                        .collect(Collectors.toList()));
            } else {
                keysBuilder.addAllPropKeys(properties.stream()
                        .map(k -> Common.PropertyKey.newBuilder().setName(k).build())
                        .collect(Collectors.toList()));
            }
        }
        return keysBuilder.build();
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

    public static ServerGremlinExecutor getServerGremlinExecutor(GremlinServer server) {
        String field = "serverGremlinExecutor";
        try {
            return (ServerGremlinExecutor) FieldUtils.readField(server, field, true);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + "not exist in step " + server.getClass(), e);
        }
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
        try {
            List<Object> results = new ArrayList<>();
            results.add(Collections.singletonMap("source", printGremlinStep(job.getSource())));
            job.getPlan().getPlan().forEach(k -> results.add(printOpr(k)));
            logger.info("{}", JsonUtils.toJson(results));
            // FileUtils.writeStringToFile(new File("plan.log"), JsonUtils.toJson(results), StandardCharsets.UTF_8, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map printGremlinStep(ByteString data) {
        try {
            Gremlin.GremlinStep step = Gremlin.GremlinStep.parseFrom(data);
            return JsonUtils.fromJson(JsonFormat.printer().print(step.toBuilder()), new TypeReference<Map>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object printOpr(PegasusClient.OperatorDef op) {
        try {
            Map<String, Object> results = new HashMap<>();
            results.put("op_kind", op.getOpKindCase().name());
            if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.ITERATE) {
                results.put("times", op.getIterate().getMaxIters());
                List<Object> body = new ArrayList<>();
                op.getIterate().getBody().getPlanList().forEach(p -> body.add(printOpr(p)));
                results.put("body", body);
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.SUBTASK) {
                List<Object> subtask = new ArrayList<>();
                op.getSubtask().getTask().getPlanList().forEach(p -> subtask.add(printOpr(p)));
                results.put("subtask", subtask);
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.ORDER) {
                results.put("limit", op.getOrder().getLimit());
                results.put("compare", printGremlinStep(op.getOrder().getCompare()));
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.GROUP) {
                results.put("map", printGremlinStep(op.getGroup().getResource()));
                results.put("accum", op.getGroup().getAccumValue());
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.UNION) {
                for (int i = 0; i < op.getUnion().getBranchesList().size(); ++i) {
                    List<Object> branches = new ArrayList<>();
                    op.getUnion().getBranches(i).getPlanList().forEach(p -> branches.add(printOpr(p)));
                    results.put(String.format("branch-%d", i), branches);
                }
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.MAP) {
                results.put("resource", printGremlinStep(op.getMap().getResource()));
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.FLAT_MAP) {
                results.put("resource", printGremlinStep(op.getFlatMap().getResource()));
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.FILTER) {
                results.put("resource", printGremlinStep(op.getFilter().getResource()));
            } else if (op.getOpKindCase() == PegasusClient.OperatorDef.OpKindCase.COMM) {
                results.put("ch_kind", op.getComm().getChKindCase());
            } else {
                // do nothing
            }
            return results;
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

    public static PegasusClient.AccumKind getAccumKind(Step groupByStep) {
        Traversal.Admin valueTraversal;
        if (groupByStep instanceof GroupStep) {
            valueTraversal = PlanUtils.getValueTraversal(groupByStep);
            // default to list
            if (valueTraversal == null || valueTraversal.getSteps().isEmpty()
                    || valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof FoldStep
                    || valueTraversal.getSteps().size() == 2 && isIdentityTraversalMap(valueTraversal.getStartStep())
                    && valueTraversal.getEndStep() instanceof FoldStep) {
                return PegasusClient.AccumKind.TO_LIST;
            } else if (valueTraversal.getSteps().size() == 1 && valueTraversal.getStartStep() instanceof CountGlobalStep) {
                return PegasusClient.AccumKind.CNT;
            } else {
                throw new UnsupportedOperationException("cannot support other value traversal " + valueTraversal);
            }
        } else if (groupByStep instanceof GroupCountStep) {
            return PegasusClient.AccumKind.CNT;
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
        } else if (type == BySubTaskStep.JoinerType.Select) {
            return Gremlin.SubTaskJoiner.newBuilder().setSelectByJoiner(Gremlin.SelectBySubJoin.newBuilder()).build();
        } else {
            throw new UnsupportedOperationException("cannot support other by joiner type " + type);
        }
    }

    public static IdMaker getTagIdMaker(Configuration conf) {
        return (IdMaker) conf.getProperty(PlanConfig.TAG_ID_MAKER);
    }

    public static String readJsonFromFile(String fileName) {
        try {
            return FileUtils.readFileToString(new File(fileName), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean getIsSimple(Step step) {
        String field = "isSimple";
        try {
            return (boolean) FieldUtils.readField(step, field, true);
            // return getPrivateField(step.getClass(), field);
        } catch (Exception e) {
            throw new RuntimeException("field " + field + " not exist in step " + step.getClass(), e);
        }
    }

    public static String convertToPropertyId(GraphStoreService graphStore, String key) {
        if (key.equals(T.label.getAccessor()) || key.equals(T.id.getAccessor())) {
            return key;
        }
        return String.valueOf(graphStore.getPropertyId(key));
    }
}
