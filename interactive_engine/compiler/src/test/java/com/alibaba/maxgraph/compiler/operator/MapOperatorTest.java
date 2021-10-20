/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.operator;

import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.Prop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.junit.Test;

import java.io.IOException;

import static com.alibaba.maxgraph.sdkcommon.compiler.custom.map.Mapper.rangeSum;

public class MapOperatorTest extends AbstractOperatorTest {

    public MapOperatorTest() throws IOException {}

    @Test
    public void testMapVOutMapPropFillInCase() {
        GraphTraversal traversal = g.V().out().map(Prop.fill("firstname")).in();
        executeTreeQuery(traversal);
    }

    @Test
    public void testMapRangeSumCase() {
        executeTreeQuery(g.V().out().map(rangeSum("firstname", 0, 2)).select(Column.values));
    }

    @Test
    public void testMapTraversalGroupCase() {
        //
        // executeTreeQuery(g.V().hasLabel("person").as("p").map(__.bothE().label().groupCount()).as("r").select("p", "r"));
    }
}
