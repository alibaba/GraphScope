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

import com.alibaba.graphscope.common.ir.tools.GraphRexBuilder;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;

public class RexNodeTypeRefresher extends RexVisitorImpl<RexNode> {
    private final RelDataType newType;
    private final GraphRexBuilder rexBuilder;

    public RexNodeTypeRefresher(RelDataType newType, GraphRexBuilder rexBuilder) {
        super(false);
        this.newType = newType;
        this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        return call;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return inputRef;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam instanceof RexGraphDynamicParam) {
            return visitGraphDynamicParam((RexGraphDynamicParam) dynamicParam);
        } else {
            return dynamicParam;
        }
    }

    private RexNode visitGraphDynamicParam(RexGraphDynamicParam graphDynamicParam) {
        return rexBuilder.makeGraphDynamicParam(
                newType, graphDynamicParam.getName(), graphDynamicParam.getIndex());
    }
}
