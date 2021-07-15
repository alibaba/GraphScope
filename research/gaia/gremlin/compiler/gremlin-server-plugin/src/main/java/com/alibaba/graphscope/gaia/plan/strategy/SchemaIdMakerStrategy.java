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
package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.graphscope.gaia.store.SchemaNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaIdMakerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(SchemaIdMakerStrategy.class);
    private static final SchemaIdMakerStrategy INSTANCE = new SchemaIdMakerStrategy();
    private GaiaConfig config;
    private GraphStoreService graphStore;

    private SchemaIdMakerStrategy() {
    }

    public static SchemaIdMakerStrategy instance(GaiaConfig config, GraphStoreService graphStore) {
        if (INSTANCE.config == null) {
            INSTANCE.config = config;
        }
        if (INSTANCE.graphStore == null) {
            INSTANCE.graphStore = graphStore;
        }
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        try {
            // label string -> label id
            List<Step> steps = traversal.getSteps();
            for (int i = 0; i < steps.size(); ++i) {
                Step step = steps.get(i);
                if (step instanceof HasContainerHolder) {
                    List<HasContainer> containers = ((HasContainerHolder) step).getHasContainers();
                    for (HasContainer container : containers) {
                        if (container.getKey().equals(T.label.getAccessor())) {
                            P predicate = container.getPredicate();
                            if (predicate.getValue() instanceof List && ((List) predicate.getValue()).get(0) instanceof String) {
                                List<String> values = (List<String>) predicate.getValue();
                                predicate.setValue(values.stream().map(k -> {
                                    if (StringUtils.isNumeric(k)) {
                                        return k;
                                    } else {
                                        long labelId = graphStore.getLabelId(k);
                                        return String.valueOf(labelId);
                                    }
                                }).collect(Collectors.toList()));
                            } else if (predicate.getValue() instanceof String) {
                                String value = (String) predicate.getValue();
                                if (StringUtils.isNumeric(value)) {
                                    predicate.setValue(value);
                                } else {
                                    long labelId = graphStore.getLabelId(value);
                                    predicate.setValue(String.valueOf(labelId));
                                }
                            } else {
                                throw new UnsupportedOperationException("hasLabel value type not support " + predicate.getValue().getClass());
                            }
                        }
                    }
                } else if (step instanceof VertexStep) {
                    String[] edgeLabels = ((VertexStep) step).getEdgeLabels();
                    for (int j = 0; j < edgeLabels.length; ++j) {
                        if (StringUtils.isNumeric(edgeLabels[j])) {
                            // do nothing
                        } else {
                            long labelId = graphStore.getLabelId(edgeLabels[j]);
                            edgeLabels[j] = String.valueOf(labelId);
                        }
                    }
                }
            }
            GraphType graphType = config.getGraphType();
            // property string -> property id
            if (graphType == GraphType.MAXGRAPH) {
                for (int i = 0; i < steps.size(); ++i) {
                    Step step = steps.get(i);
                    if (step instanceof HasContainerHolder) {
                        List<HasContainer> containers = ((HasContainerHolder) step).getHasContainers();
                        for (HasContainer container : containers) {
                            container.setKey(PlanUtils.convertToPropertyId(graphStore, container.getKey()));
                        }
                    } else if (step instanceof PropertiesStep || step instanceof PropertyMapStep) {
                        String[] oldKeys;
                        if (step instanceof PropertiesStep) {
                            oldKeys = ((PropertiesStep) step).getPropertyKeys();
                        } else {
                            oldKeys = ((PropertyMapStep) step).getPropertyKeys();
                        }
                        String[] newKeys = Arrays.stream(oldKeys).map(k -> PlanUtils.convertToPropertyId(graphStore, k))
                                .toArray(String[]::new);
                        FieldUtils.writeField(step, "propertyKeys", newKeys, true);
                    } else if (step instanceof ByModulating) {
                        TraversalParent byParent = (TraversalParent) step;
                        for (Traversal.Admin k : byParent.getLocalChildren()) {
                            if (k instanceof ElementValueTraversal) {
                                ElementValueTraversal value = (ElementValueTraversal) k;
                                String propertyId = PlanUtils.convertToPropertyId(graphStore, value.getPropertyKey());
                                FieldUtils.writeField(value, "propertyKey", propertyId, true);
                            }
                        }
                    }
                }
            }
        } catch (SchemaNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
