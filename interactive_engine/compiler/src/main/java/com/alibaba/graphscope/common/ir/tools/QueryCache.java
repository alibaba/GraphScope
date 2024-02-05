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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class QueryCache {
    private final LoadingCache<Key, Value> cache;
    private final GraphPlanner graphPlanner;

    public QueryCache(Configs configs, GraphPlanner graphPlanner) {
        this.graphPlanner = graphPlanner;
        int cacheSize = FrontendConfig.QUERY_CACHE_SIZE.get(configs);
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .build(
                                CacheLoader.from(
                                        key -> {
                                            PhysicalPlan physicalPlan =
                                                    key.plannerInstance.planPhysical(
                                                            key.logicalPlan);
                                            GraphPlanner.Summary summary =
                                                    new GraphPlanner.Summary(
                                                            key.logicalPlan, physicalPlan);
                                            return new Value(summary, null);
                                        }));
    }

    public class Key {
        public final GraphPlanner.PlannerInstance plannerInstance;
        public final LogicalPlan logicalPlan;

        public Key(String query, IrMeta irMeta) {
            this.plannerInstance = Objects.requireNonNull(graphPlanner.instance(query, irMeta));
            this.logicalPlan = Objects.requireNonNull(this.plannerInstance.planLogical());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key that = (Key) o;
            return Objects.equals(logicalPlan, that.logicalPlan);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logicalPlan);
        }
    }

    public Key createKey(String query, IrMeta irMeta) {
        return new Key(query, irMeta);
    }

    public static class Value {
        public final GraphPlanner.Summary summary;
        public @Nullable Result result;

        public Value(GraphPlanner.Summary summary, Result result) {
            this.summary = Objects.requireNonNull(summary);
            this.result = result;
        }
    }

    public static class Result<T> {
        public final long queryId;
        public final List<T> records;
        public boolean isCompleted;

        public Result(long queryId, List<T> records, boolean isCompleted) {
            this.queryId = queryId;
            this.records = Objects.requireNonNull(records);
            this.isCompleted = isCompleted;
        }
    }

    public Value get(Key key) throws ExecutionException {
        return cache.get(key);
    }
}
