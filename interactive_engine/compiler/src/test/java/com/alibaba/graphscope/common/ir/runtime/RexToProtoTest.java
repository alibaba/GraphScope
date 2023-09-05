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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.LabelConfig;
import com.alibaba.graphscope.common.ir.tools.config.SourceConfig;
import com.alibaba.graphscope.common.utils.FileUtils;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;

public class RexToProtoTest {
    // convert expression 'a.age - (1-a.age)' to proto
    @Test
    public void test_expression_with_brace() throws Exception {
        GraphBuilder builder =
                Utils.mockGraphBuilder()
                        .source(
                                new SourceConfig(
                                        GraphOpt.Source.VERTEX,
                                        new LabelConfig(false).addLabel("person"),
                                        "a"));

        RexNode braceExpr =
                builder.call(
                        GraphStdOperatorTable.MINUS,
                        builder.variable("a", "age"),
                        builder.call(
                                GraphStdOperatorTable.MINUS,
                                builder.literal(1),
                                builder.variable("a", "age")));
        RexToProtoConverter converter = new RexToProtoConverter(true, false);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("proto/expression_with_brace.json"),
                JsonFormat.printer().print(braceExpr.accept(converter)));
    }
}
