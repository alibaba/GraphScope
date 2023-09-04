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

package com.alibaba.graphscope.cypher.antlr4;

import com.alibaba.graphscope.common.ir.tools.LogicalPlan;

import org.junit.Assert;
import org.junit.Test;

public class CallProcedureTest {
    @Test
    public void match_1_test() {
        LogicalPlan logicalPlan = Utils.evalLogicalPlan("Call ldbc_ic2(10l, 20120112l)");
        Assert.assertEquals("ldbc_ic2(10:BIGINT, 20120112:BIGINT)", logicalPlan.explain().trim());
        Assert.assertEquals(
                "RecordType(CHAR(1) name)", logicalPlan.getProcedureCall().getType().toString());
    }
}
