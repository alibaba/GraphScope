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
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class QueryCache implements IrMetaTracker {
    private static final Logger logger = LoggerFactory.getLogger(QueryCache.class);
    private final LoadingCache<Key, Value> cache;

    public QueryCache(Configs configs) {
        int cacheSize = FrontendConfig.QUERY_CACHE_SIZE.get(configs);
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(cacheSize)
                        .build(
                                CacheLoader.from(
                                        key ->
                                                new Value(
                                                        key.instance.plan(),
                                                        null,
                                                        ImmutableMap.of(
                                                                "instance", key.instance))));
    }

    @Override
    public void onSchemaChanged(IrMeta meta) {
        cache.invalidateAll();
    }

    @Override
    public void onStatsChanged(IrMetaStats stats) {
        // do nothing
    }

    public class Key {
        public final GraphPlanner.PlannerInstance instance;
        public final LogicalPlan logicalPlan;

        public Key(GraphPlanner.PlannerInstance instance) {
            this.instance = instance;
            this.logicalPlan = instance.getParsedPlan();
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

    public Key createKey(GraphPlanner.PlannerInstance instance) {
        return new Key(instance);
    }

    public static class Value {
        public final GraphPlanner.Summary summary;
        public @Nullable Result result;
        public final Map<String, Object> debugInfo;

        public Value(GraphPlanner.Summary summary, Result result) {
            this(summary, result, Maps.newHashMap());
        }

        public Value(GraphPlanner.Summary summary, Result result, Map<String, Object> debugInfo) {
            this.summary = Objects.requireNonNull(summary);
            this.result = result;
            this.debugInfo = debugInfo;
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
