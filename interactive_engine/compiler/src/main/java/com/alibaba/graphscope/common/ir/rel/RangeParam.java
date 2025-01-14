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

package com.alibaba.graphscope.common.ir.rel;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class RangeParam extends Pair<RexNode, RexNode> {
    public RangeParam(RexNode offset, RexNode fetch) {
        super(offset, fetch);
    }

    @Override
    public String toString() {
        RexNode offset = left;
        RexNode fetch = right;
        // default value of offset is 0
        String offsetStr = "0";
        if (offset instanceof RexLiteral) {
            offsetStr = String.valueOf(((Number) ((RexLiteral) offset).getValue()).intValue());
        } else if (offset != null) {
            offsetStr = offset.toString();
        }
        // default value of fetch is -1, which mean no upper bound
        String fetchStr = "-1";
        if (fetch instanceof RexLiteral) {
            fetchStr = String.valueOf(((Number) ((RexLiteral) fetch).getValue()).intValue());
        } else if (fetch != null) {
            fetchStr = fetch.toString();
        }
        return "<" + offsetStr + ", " + fetchStr + ">";
    }
}
