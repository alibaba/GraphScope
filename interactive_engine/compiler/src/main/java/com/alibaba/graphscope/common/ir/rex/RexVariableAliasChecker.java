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

import org.apache.calcite.rex.*;

import java.util.List;

/**
 * check whether all variables in an expression have at least one alias in the {@code aliasIds}
 */
public class RexVariableAliasChecker extends RexVisitorImpl<Boolean> {
    private List<Integer> aliasIds;

    public RexVariableAliasChecker(boolean deep, List<Integer> aliasId) {
        super(deep);
        this.aliasIds = aliasId;
    }

    @Override
    public Boolean visitCall(RexCall call) {
        if (this.deep) {
            for (RexNode operand : call.getOperands()) {
                if (!operand.accept(this)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
        return true;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        return (inputRef instanceof RexGraphVariable)
                ? visitGraphVariable((RexGraphVariable) inputRef)
                : true;
    }

    public Boolean visitGraphVariable(RexGraphVariable variable) {
        return this.aliasIds.contains(variable.getAliasId());
    }
}
