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

import org.junit.Test;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class EarlyStopOperatorTest extends AbstractOperatorTest {

    public EarlyStopOperatorTest() throws IOException {}

    @Test
    public void testEarlyOutOutLimitCase() {
        executeTreeQuery(g.V().out().out().out().limit(30));
    }

    @Test
    public void testEarlyOutLimitOutLimitCase() {
        executeTreeQuery(g.V().out().out().limit(20).out().limit(30));
    }

    @Test
    public void testEarlyRepeatBothLimitCase() {
        executeTreeQuery(g.V().out().repeat(both()).times(100).out().out().limit(30));
    }

    @Test
    public void testEarlyRepeatOutInLimitCase() {
        executeTreeQuery(g.V().out().repeat(out().in()).times(100).out().out().limit(30));
    }

    @Test
    public void testDisableSnapshotCase() {
        executeTreeQuery(g.disableSnapshot().V().out());
    }
}
