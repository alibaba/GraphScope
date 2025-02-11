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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.fetcher.StaticIrMetaFetcher;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.LogicalPlanFactory;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.sdk.PlanUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class QueryCacheTest {
    // test hash code of query cache key
    @Test
    public void query_cache_1_test() {
        Configs configs = new Configs(ImmutableMap.of("query.cache.size", "1"));
        GraphPlanner graphPlanner =
                new GraphPlanner(
                        configs, new LogicalPlanFactory.Cypher(), new GraphRelOptimizer(configs));
        QueryCache cache = new QueryCache(configs);
        QueryCache.Key key1 =
                cache.createKey(
                        graphPlanner.instance("Match (n {name: 'ma'}) Return n", Utils.schemaMeta));
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[n], fusedFilter=[[=(_.name, _UTF-8'ma')]], opt=[VERTEX])",
                key1.logicalPlan.explain().trim());
        QueryCache.Key key2 =
                cache.createKey(
                        graphPlanner.instance("Match (n {name: 'ma'}) Return n", Utils.schemaMeta));
        QueryCache.Key key3 =
                cache.createKey(
                        graphPlanner.instance("Match (n {age: 10}) Return n", Utils.schemaMeta));
        Assert.assertEquals(key1, key2);
        Assert.assertNotEquals(key1, key3);
    }

    // test evict strategy of query cache
    @Test
    public void query_cache_2_test() throws Exception {
        Configs configs = new Configs(ImmutableMap.of("query.cache.size", "1"));
        GraphPlanner graphPlanner =
                new GraphPlanner(
                        configs, new LogicalPlanFactory.Cypher(), new GraphRelOptimizer(configs));
        QueryCache cache = new QueryCache(configs);
        QueryCache.Key key1 =
                cache.createKey(
                        graphPlanner.instance("Match (n {name: 'ma'}) Return n", Utils.schemaMeta));
        QueryCache.Key key2 =
                cache.createKey(
                        graphPlanner.instance("Match (n {age: 10}) Return n", Utils.schemaMeta));
        QueryCache.Key key3 =
                cache.createKey(
                        graphPlanner.instance("Match (n {name: 'ma'}) Return n", Utils.schemaMeta));
        QueryCache.Value value1 = cache.get(key1);
        QueryCache.Value value2 = cache.get(key2);
        QueryCache.Value value3 = cache.get(key3);
        // value1 should have been evicted due to max size is 1
        Assert.assertTrue(value1 != value3);
    }

    // test cache invalidation after schema update
    @Test
    public void query_cache_schema_update_test() throws Exception {
        Configs configs = new Configs(ImmutableMap.of());
        GraphPlanner graphPlanner =
                new GraphPlanner(
                        configs, new LogicalPlanFactory.Cypher(), new GraphRelOptimizer(configs));
        QueryCache cache = new QueryCache(configs);

        // before update, the label id of person is 0
        IrMeta metaBefore =
                createIrMeta(
                        "schema:\n"
                                + "  vertex_types:\n"
                                + "    - type_name: person\n"
                                + "      type_id: 0\n"
                                + "      properties:\n"
                                + "      primary_keys:",
                        configs,
                        graphPlanner,
                        cache);
        QueryCache.Key key1 =
                cache.createKey(graphPlanner.instance("Match (n:person) Return n", metaBefore));
        QueryCache.Value value1 = cache.get(key1);
        RelNode plan1 = value1.summary.getLogicalPlan().getRegularQuery();
        GraphSchemaType schemaType1 =
                (GraphSchemaType) plan1.getInput(0).getRowType().getFieldList().get(0).getType();
        Assert.assertEquals(
                0, (int) schemaType1.getLabelType().getLabelsEntry().get(0).getLabelId());

        // after update, the label id of person is 1, the cache should be invalidated and the plan
        // should be recompiled
        IrMeta metaAfter =
                createIrMeta(
                        "schema:\n"
                                + "  vertex_types:\n"
                                + "    - type_name: software\n"
                                + "      type_id: 0\n"
                                + "      properties:\n"
                                + "      primary_keys:\n"
                                + "    - type_name: person\n"
                                + "      type_id: 1\n"
                                + "      properties:\n"
                                + "      primary_keys:",
                        configs,
                        graphPlanner,
                        cache);
        QueryCache.Key key2 =
                cache.createKey(graphPlanner.instance("Match (n:person) Return n", metaAfter));
        QueryCache.Value value2 = cache.get(key2);
        RelNode plan2 = value2.summary.getLogicalPlan().getRegularQuery();
        GraphSchemaType schemaType2 =
                (GraphSchemaType) plan2.getInput(0).getRowType().getFieldList().get(0).getType();
        Assert.assertEquals(
                1, (int) schemaType2.getLabelType().getLabelsEntry().get(0).getLabelId());
    }

    private IrMeta createIrMeta(
            String schemaYaml, Configs configs, GraphPlanner graphPlanner, QueryCache cache)
            throws Exception {
        IrMetaReader reader = new PlanUtils.StringMetaReader(schemaYaml, "", configs);
        return (new StaticIrMetaFetcher(
                        reader, ImmutableList.of(graphPlanner.getOptimizer(), cache)))
                .fetch()
                .get();
    }
}
