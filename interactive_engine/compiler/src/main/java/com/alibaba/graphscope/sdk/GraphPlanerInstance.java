/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.sdk;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.IrMetaFetcher;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.planner.PlannerGroupManager;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlanFactory;

public class GraphPlanerInstance {
    private final GraphPlanner planner;
    private final IrMeta meta;

    private static GraphPlanerInstance instance;

    public GraphPlanerInstance(GraphPlanner planner, IrMeta meta) {
        this.planner = planner;
        this.meta = meta;
    }

    public static synchronized GraphPlanerInstance getInstance(
            String configPath, GraphPlanner.IrMetaFetcherFactory metaFetcherFactory)
            throws Exception {
        if (instance == null) {
            Configs configs = Configs.Factory.create(configPath);
            GraphRelOptimizer optimizer =
                    new GraphRelOptimizer(configs, PlannerGroupManager.Static.class);
            IrMetaFetcher metaFetcher =
                    metaFetcherFactory.create(configs, optimizer.getGlogueHolder());
            GraphPlanner planner =
                    new GraphPlanner(configs, new LogicalPlanFactory.Cypher(), optimizer);
            instance = new GraphPlanerInstance(planner, metaFetcher.fetch().get());
        }
        return instance;
    }

    public GraphPlanner getPlanner() {
        return planner;
    }

    public IrMeta getMeta() {
        return meta;
    }
}
