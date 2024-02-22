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

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;

public class RexConverterAdaptor extends RexVisitorImpl<RexNode> {
    private final RexVisitor<RexNode> convertor;

    public RexConverterAdaptor(boolean deep, RexVisitor<RexNode> convertor) {
        super(deep);
        this.convertor = convertor;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        if (inputRef instanceof RexGraphVariable) {
            return visitGraphVariable((RexGraphVariable) inputRef);
        } else {
            return convertor.visitInputRef(inputRef);
        }
    }

    private RexNode visitGraphVariable(RexGraphVariable variable) {
        RexInputRef original = (RexInputRef) convertor.visitInputRef(variable);
        return variable.getProperty() == null
                ? RexGraphVariable.of(
                        variable.getAliasId(),
                        original.getIndex(),
                        variable.getName(),
                        variable.getType())
                : RexGraphVariable.of(
                        variable.getAliasId(),
                        variable.getProperty(),
                        original.getIndex(),
                        variable.getName(),
                        variable.getType());
    }
}
