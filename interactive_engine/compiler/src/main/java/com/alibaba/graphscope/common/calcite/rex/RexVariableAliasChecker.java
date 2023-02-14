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

package com.alibaba.graphscope.common.calcite.rex;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.List;

/**
 * check whether all variables in an expression have at least one alias in the{@code aliasIds}
 */
public class RexVariableAliasChecker extends RexVisitorImpl<Boolean> {
    private boolean isAll;
    private List<Integer> aliasIds;

    public RexVariableAliasChecker(boolean deep, List<Integer> aliasId) {
        super(deep);
        this.aliasIds = aliasId;
        this.isAll = true;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        if (inputRef instanceof RexGraphVariable) {
            if (!this.aliasIds.contains(((RexGraphVariable) inputRef).getAliasId())) {
                this.isAll = false;
            }
        }
        return null;
    }

    public boolean isAll() {
        return isAll;
    }
}
