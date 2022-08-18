/*
 * Copyright 2022 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.integration;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.dedup;

import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public abstract class IrGremlinQueryTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<Object, Long>>
            get_g_V_group_by_by_dedup_count_order_by_key();

    @Test
    public void g_V_group_by_by_dedup_count_test() {
        Traversal<Vertex, Map<Object, Long>> traversal =
                this.get_g_V_group_by_by_dedup_count_order_by_key();
        this.printTraversalForm(traversal);

        String expected =
                "{v[1]=1, v[2]=1, v[72057594037927939]=1, v[4]=1, v[72057594037927941]=1, v[6]=1}";

        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(expected, result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    public static class Traversals extends IrGremlinQueryTest {

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_group_by_by_dedup_count_order_by_key() {
            return (IrCustomizedTraversal)
                    g.V().group()
                            .by()
                            .by(dedup().count())
                            .order()
                            .by(__.select(Column.keys).values("id"), Order.asc);
        }
    }
}
