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

package com.alibaba.graphscope.common.ir.rex;

import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.regex.Pattern;

/**
 * check property existence for each {@code RexGraphVariable} in a expression
 */
public class RexPropertyChecker extends RexVisitorImpl<Void> {
    private final GraphBuilder builder;

    public RexPropertyChecker(boolean deep, GraphBuilder builder) {
        super(deep);
        this.builder = builder;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
        if (inputRef instanceof RexGraphVariable) {
            RexGraphVariable variable = (RexGraphVariable) inputRef;
            String[] splits = variable.getName().split(Pattern.quote(AliasInference.DELIMITER));
            if (splits.length > 1) {
                String alias = splits[0];
                String property = splits[1];
                if (alias == null
                        || alias.isEmpty()
                        || alias.equals(AliasInference.DEFAULT_NAME)
                        || alias.equals(AliasInference.SIMPLE_NAME(AliasInference.DEFAULT_NAME))) {
                    builder.variable(null, property);
                } else {
                    builder.variable(alias, property);
                }
            }
        }
        return null;
    }
}
