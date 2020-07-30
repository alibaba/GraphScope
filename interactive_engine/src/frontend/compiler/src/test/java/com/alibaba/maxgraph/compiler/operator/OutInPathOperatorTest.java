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
package com.alibaba.maxgraph.compiler.operator;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.io.IOException;

import static com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program.ratio;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;

public class OutInPathOperatorTest extends AbstractOperatorTest {
    public OutInPathOperatorTest() throws IOException {
    }

    @Test
    public void testTree_V_out_in_out_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out());
    }

    @Test
    public void testTree_V_out_in_out_path_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out().path());
    }

    @Test
    public void testTree_V_out_in_out_pathByIdName_case() {
        executeTreeQuery(g.V().out().in("person_knows_person").out().path().by().by(T.id).by("firstname"));
    }

    @Test
    public void testOutRatioInOutCase() {
        executeTreeQuery(g.V().out().in().by(ratio(gt(0.95))).out());
    }

    @Test
    public void testOutRatioIneOutVCase() {
        executeTreeQuery(g.V().out().inE().outV().by(ratio(gt(0.85))).out());
    }

    @Test
    public void testOutInRatioCase() {
        executeTreeQuery(g.V().out().in().by(ratio(gt(0.6))));
    }
}
