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
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.QueryCache;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.cypher.antlr4.parser.CypherAntlr4Parser;
import com.alibaba.graphscope.cypher.antlr4.visitor.LogicalPlanVisitor;
import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

public class QueryCacheTest {
    // test hash code of query cache key
    @Test
    public void query_cache_1_test() {
        Configs configs = new Configs(ImmutableMap.of("query.cache.size", "1"));
        GraphPlanner graphPlanner =
                new GraphPlanner(
                        configs,
                        (GraphBuilder builder, IrMeta irMeta, String q) ->
                                new LogicalPlanVisitor(builder, irMeta)
                                        .visit(new CypherAntlr4Parser().parse(q)));
        QueryCache cache = new QueryCache(configs, graphPlanner);
        QueryCache.Key key1 = cache.createKey("Match (n {name: 'ma'}) Return n", Utils.schemaMeta);
        Assert.assertEquals(
                "GraphLogicalProject(n=[n], isAppend=[false])\n"
                    + "  GraphLogicalSource(tableConfig=[{isAll=true, tables=[software, person]}],"
                    + " alias=[n], fusedFilter=[[=(DEFAULT.name, _UTF-8'ma')]], opt=[VERTEX])",
                key1.logicalPlan.explain().trim());
        QueryCache.Key key2 = cache.createKey("Match (n {name: 'ma'}) Return n", Utils.schemaMeta);
        QueryCache.Key key3 = cache.createKey("Match (n {age: 10}) Return n", Utils.schemaMeta);
        Assert.assertEquals(key1, key2);
        Assert.assertNotEquals(key1, key3);
    }

    // test evict strategy of query cache
    @Test
    public void query_cache_2_test() throws Exception {
        Configs configs = new Configs(ImmutableMap.of("query.cache.size", "1"));
        GraphPlanner graphPlanner =
                new GraphPlanner(
                        configs,
                        (GraphBuilder builder, IrMeta irMeta, String q) ->
                                new LogicalPlanVisitor(builder, irMeta)
                                        .visit(new CypherAntlr4Parser().parse(q)));
        QueryCache cache = new QueryCache(configs, graphPlanner);
        QueryCache.Key key1 = cache.createKey("Match (n {name: 'ma'}) Return n", Utils.schemaMeta);
        QueryCache.Key key2 = cache.createKey("Match (n {age: 10}) Return n", Utils.schemaMeta);
        QueryCache.Key key3 = cache.createKey("Match (n {name: 'ma'}) Return n", Utils.schemaMeta);
        QueryCache.Value value1 = cache.get(key1);
        QueryCache.Value value2 = cache.get(key2);
        QueryCache.Value value3 = cache.get(key3);
        // value1 should have been evicted due to max size is 1
        Assert.assertTrue(value1 != value3);
    }
}
