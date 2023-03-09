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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.*;

import org.junit.Assert;
import org.junit.Test;

public class GetVTest {
    // source("person").as("x").expand("knows").as("y").getV("person").as("x") -> invalid
    @Test
    public void getV_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder();
        try {
            builder.source(
                            new SourceConfig(
                                    GraphOpt.Source.VERTEX,
                                    new LabelConfig(false).addLabel("person"),
                                    "x"))
                    .expand(
                            new ExpandConfig(
                                    GraphOpt.Expand.OUT,
                                    new LabelConfig(false).addLabel("knows"),
                                    "y"))
                    .getV(
                            new GetVConfig(
                                    GraphOpt.GetV.END,
                                    new LabelConfig(false).addLabel("person"),
                                    "x"))
                    .build();
        } catch (IllegalArgumentException e) {
            // expected error
            return;
        }
        Assert.fail("should have thrown error for duplicated alias");
    }
}
