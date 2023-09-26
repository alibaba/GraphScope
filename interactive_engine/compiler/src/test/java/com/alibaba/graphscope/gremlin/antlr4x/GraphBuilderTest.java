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

package com.alibaba.graphscope.gremlin.antlr4x;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gremlin.antlr4x.parser.GremlinAntlr4Parser;
import com.alibaba.graphscope.gremlin.antlr4x.visitor.GraphBuilderVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class GraphBuilderTest {
    @Test
    public void g_V_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(builder);
        ParseTree parseTree =
                new GremlinAntlr4Parser()
                        .parse("g.V().hasLabel('person').hasId(1, 2, 3).hasNot('name')");
        RelNode node = visitor.visit(parseTree).build();
        System.out.println(node.explain());
    }

    @Test
    public void g_V_out_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        GraphBuilderVisitor visitor = new GraphBuilderVisitor(builder);
        ParseTree parseTree =
                new GremlinAntlr4Parser()
                        .parse("g.V().hasLabel('person').as('a').out().hasLabel('person').as('b').select('a', 'b').by('name').by()");
        RelNode node = visitor.visit(parseTree).build();
        LogicalPlan plan = new LogicalPlan(node);
        System.out.println(node.explain());
        System.out.println(plan.getOutputType());
    }

}
