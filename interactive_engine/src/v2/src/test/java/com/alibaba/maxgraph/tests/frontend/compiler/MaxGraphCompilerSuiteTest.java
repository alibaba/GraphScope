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
package com.alibaba.maxgraph.tests.frontend.compiler;

import com.alibaba.maxgraph.tests.frontend.compiler.operator.AggregateOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.BranchOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.ChainSourceOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.ChooseOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.ContextOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.CostOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.CountOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.DedupOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.EdgeOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.FilterOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.FlatMapOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.FoldOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.GroupCountOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.GroupOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.HasLabelIdOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.OrderOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.OutInPathOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.PathOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.PropertiesOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.RangeOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.RepeatOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.SampleOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.SelectOneOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.StoreOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.UnionOperatorTest;
import com.alibaba.maxgraph.tests.frontend.compiler.operator.WhereOperatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
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
        ChainSourceOperatorTest.class,
        SelectOneOperatorTest.class,
        CountOperatorTest.class,
        BranchOperatorTest.class,
        FoldOperatorTest.class,
        StoreOperatorTest.class,
        EdgeOperatorTest.class,
        ContextOperatorTest.class,
        AggregateOperatorTest.class,
        DedupOperatorTest.class,
        ChooseOperatorTest.class,
        FlatMapOperatorTest.class,
        CostOperatorTest.class,
        SampleOperatorTest.class,
})
public class MaxGraphCompilerSuiteTest {
}
