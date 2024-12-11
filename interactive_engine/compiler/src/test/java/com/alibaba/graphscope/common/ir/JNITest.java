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

import com.alibaba.graphscope.common.jna._native.GraphPlannerJNI;
import com.alibaba.graphscope.common.jna._native.JNIPlan;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;

import org.junit.Test;

public class JNITest {
    @Test
    public void test() throws Exception {
        JNIPlan objects =
                GraphPlannerJNI.compilePlan(
                        "conf/ir.compiler.properties", "Match (n) Return n, count(n)", "", "");
        GraphAlgebraPhysical.PhysicalPlan plan =
                GraphAlgebraPhysical.PhysicalPlan.parseFrom(objects.physicalBytes);
        System.out.println(plan);
        String resultSchema = objects.resultSchemaYaml;
        System.out.println(resultSchema);
    }
}
