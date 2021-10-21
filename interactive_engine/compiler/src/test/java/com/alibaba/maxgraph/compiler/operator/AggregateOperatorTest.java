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

public class AggregateOperatorTest extends AbstractOperatorTest {

    public AggregateOperatorTest() throws IOException {}

    @Test
    public void testAggregateOutInCount() {
        executeTreeQuery(g.V().out().group().by().by());
    }

    @Test
    public void testAggregateGroupByList() {
        //        executeTreeQuery(g.V().has("name",
        // "marko").repeat(__.bothE().where(P.without("e")).aggregate("e").otherV()).emit().path());
    }
}
