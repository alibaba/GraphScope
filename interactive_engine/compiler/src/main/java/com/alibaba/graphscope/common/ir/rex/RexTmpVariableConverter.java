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

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;

import org.apache.calcite.rex.*;

import java.util.ArrayList;
import java.util.List;

// build from RexTmpVariable to RexGraphVariable
public class RexTmpVariableConverter extends RexVisitorImpl<RexNode> {
    private GraphBuilder builder;

    public RexTmpVariableConverter(boolean deep, GraphBuilder builder) {
        super(deep);
        this.builder = builder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (!this.deep) {
            return null;
        } else {
            List<RexNode> results = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                results.add(operand.accept(this));
            }
            return builder.call(call.getOperator(), results);
        }
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return (inputRef instanceof RexTmpVariable)
                ? visitTmpVariable((RexTmpVariable) inputRef)
                : inputRef;
    }

    public RexNode visitTmpVariable(RexTmpVariable tmpVar) {
        return (tmpVar.getProperty() == null)
                ? builder.variable(tmpVar.getAlias())
                : builder.variable(tmpVar.getAlias(), tmpVar.getProperty());
    }
}
