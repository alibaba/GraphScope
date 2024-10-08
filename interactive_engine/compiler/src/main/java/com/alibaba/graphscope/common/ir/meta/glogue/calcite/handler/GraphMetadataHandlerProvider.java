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

package com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler;

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;

public class GraphMetadataHandlerProvider implements MetadataHandlerProvider {
    private final RelOptPlanner optPlanner;
    private final GlogueQuery glogueQuery;
    private final PlannerConfig plannerConfig;

    public GraphMetadataHandlerProvider(
            RelOptPlanner optPlanner, GlogueQuery glogueQuery, PlannerConfig plannerConfig) {
        this.optPlanner = optPlanner;
        this.glogueQuery = glogueQuery;
        this.plannerConfig = plannerConfig;
    }

    @Override
    public MetadataHandler handler(Class handlerClass) {
        if (handlerClass.equals(BuiltInMetadata.RowCount.Handler.class)) {
            return new GraphRowCountHandler(this.optPlanner, this.glogueQuery);
        } else if (handlerClass.equals(ExternalMetaData.GlogueEdges.Handler.class)) {
            return new GraphGlogueEdgesHandler(this.glogueQuery);
        } else if (handlerClass.equals(BuiltInMetadata.NonCumulativeCost.Handler.class)) {
            return new GraphNonCumulativeCostHandler(this.optPlanner, this.plannerConfig);
        } else if (handlerClass.equals(BuiltInMetadata.Selectivity.Handler.class)) {
            return new GraphSelectivityHandler();
        } else {
            return (MetadataHandler)
                    handlerClass.cast(
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class[] {handlerClass},
                                    (proxy, method, args) -> {
                                        RelNode r =
                                                Objects.requireNonNull(
                                                        (RelNode) args[0], "(RelNode) args[0]");
                                        throw new JaninoRelMetadataProvider.NoHandler(r.getClass());
                                    }));
        }
    }

    @Override
    public MetadataHandler revise(Class handlerClass) {
        List<MetadataHandler> handlers = DefaultRelMetadataProvider.INSTANCE.handlers(handlerClass);
        Preconditions.checkArgument(
                !handlers.isEmpty(),
                "handlerClass=" + handlerClass + " not exist in default meta data provider");
        MetadataHandler first = handlers.get(0);
        InvocationHandler handler =
                (proxy, method, args) -> {
                    RelNode rel = Objects.requireNonNull((RelNode) args[0], "rel must not be null");
                    for (Method method1 : first.getClass().getMethods()) {
                        Class<?>[] types = method1.getParameterTypes();
                        if (method.getName().equals(method1.getName())
                                && types.length > 0
                                && types[0].isAssignableFrom(rel.getClass())) {
                            return method1.invoke(first, args);
                        }
                    }
                    throw new IllegalArgumentException(
                            "can not found any matched functions for method=" + method.getName());
                };
        return (MetadataHandler)
                handlerClass.cast(
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(), new Class[] {handlerClass}, handler));
    }

    public GlogueQuery getGlogueQuery() {
        return this.glogueQuery;
    }
}
