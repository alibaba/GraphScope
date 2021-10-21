/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph;

import com.alibaba.maxgraph.compiler.cost.CostDataStatisticsTest;
import com.alibaba.maxgraph.compiler.dfs.DfsTraversalTest;
import com.alibaba.maxgraph.compiler.operator.*;
import com.alibaba.maxgraph.compiler.prepare.PrepareTestCase;
import com.alibaba.maxgraph.compiler.utils.ReflectionUtilsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        PrepareTestCase.class,

        ReflectionUtilsTest.class,

        GroupOperatorTest.class,
        GroupCountOperatorTest.class,
        PropertiesOperatorTest.class,
        HasLabelIdOperatorTest.class,
        PathOperatorTest.class,
        WhereOperatorTest.class,
        OrderOperatorTest.class,
        RangeOperatorTest.class,
        FilterOperatorTest.class,
        OutInPathOperatorTest.class,
        UnionOperatorTest.class,
        RepeatOperatorTest.class,
        DfsTraversalTest.class,
        DfsOperatorTest.class,
        CacheOperatorTest.class,
        ChainSourceOperatorTest.class,
        MapOperatorTest.class,
        CountOperatorTest.class,
        BranchOperatorTest.class,
        FoldOperatorTest.class,
        EarlyStopOperatorTest.class,
        StoreOperatorTest.class,
        EdgeOperatorTest.class,
        SubgraphOperatorTest.class,
        ContextOperatorTest.class,
        AggregateOperatorTest.class,
        DedupOperatorTest.class,
        ChooseOperatorTest.class,
        FlatMapOperatorTest.class,
        PullGraphOperatorTest.class,
        CostDataStatisticsTest.class,
        CostOperatorTest.class,
        LambdaOperatorTest.class,
})
public class MaxGraphCompilerSuiteTest {
}
