/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.cypher.antlr4.Utils;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.Assert;
import org.junit.Test;

public class TypeConvertorTest {
    @Test
    public void string_test() {
        LogicalPlan plan =
                Utils.evalLogicalPlan("Match (n) Return n.name1;", "schema/type_test.yaml");
        RelDataType type = plan.getOutputType();
        // convert long text to VARCHAR with unlimited length, the length is omitted
        Assert.assertEquals("RecordType(VARCHAR name1)", type.toString());

        plan = Utils.evalLogicalPlan("Match (n) Return n.name2", "schema/type_test.yaml");
        type = plan.getOutputType();
        // convert fixed length text to CHAR, the specified length is 255
        Assert.assertEquals("RecordType(CHAR(255) name2)", type.toString());

        plan = Utils.evalLogicalPlan("Match (n) Return n.name3", "schema/type_test.yaml");
        type = plan.getOutputType();
        // convert varied length text to VARCHAR, the specified length is 255
        Assert.assertEquals("RecordType(VARCHAR(255) name3)", type.toString());
    }

    @Test
    public void decimal_test() {
        LogicalPlan plan =
                Utils.evalLogicalPlan("Match (n) Return n.value1;", "schema/type_test.yaml");
        RelDataType type = plan.getOutputType();
        // convert decimal to DECIMAL, the specified precision is 4 and scale is 2
        Assert.assertEquals("RecordType(DECIMAL(4, 2) value1)", type.toString());
    }
}
