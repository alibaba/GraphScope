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
import com.alibaba.graphscope.gaia.store.GraphStoreService;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GraphTraversalStrategies extends DefaultTraversalStrategies {
    public static List<TraversalStrategy> strategies;

    public static int PROPERTY_SHUFFLE_PRIORITY = 6;
    public static int ORDER_GUARANTEE_PRIORITY = 10;

    public GraphTraversalStrategies(GaiaConfig config, GraphStoreService graphStore) {
        strategies = new ArrayList<>();
        strategies.add(RemoveUselessStepStrategy.instance());
        // strategies.add(RepeatUnrollStrategy.instance());
        strategies.add(PathLocalCountStrategy.instance());
        strategies.add(WhereTraversalStrategy.instance());
        strategies.add(SchemaIdMakerStrategy.instance(config, graphStore));
        strategies.add(GaiaGraphStepStrategy.instance(graphStore));
        strategies.add(IncidentToAdjacentStrategy.instance());
        strategies.add(OrderGlobalLimitStrategy.instance());
        strategies.add(PropertyShuffleStrategy.instance());
        strategies.add(BySubTraversalStrategy.instance());
        strategies.add(EarlyLimitStrategy.instance());
        strategies.add(WhereEndLimitStrategy.instance());
        strategies.add(OrderGuaranteeStrategy.instance());
        strategies.add(PhysicalPlanUnfoldStrategy.instance());
        strategies.add(ValueTraversalParentStrategy.instance());
        strategies.add(CachePropStepStrategy.instance(config));
    }

    @Override
    public TraversalStrategies removeStrategies(final Class... strategyClasses) {
        List<Class> toRemoveList = Arrays.asList(strategyClasses);
        Iterator<TraversalStrategy> it = strategies.iterator();
        while (it.hasNext()) {
            TraversalStrategy current = it.next();
            if (toRemoveList.contains(current.getClass())) {
                it.remove();
            }
        }
        return this;
    }

    public void addStrategyByPriority(int priority, TraversalStrategy toAdd) {
        if (!strategies.contains(toAdd)) {
            strategies.add(priority, toAdd);
        }
    }

    @Override
    public void applyStrategies(final Traversal.Admin<?, ?> traversal) {
        strategies.forEach(s -> s.apply(traversal));
    }
}
