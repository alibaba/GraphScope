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

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class ExpandOp extends InterOpBase {
    public ExpandOp() {
        super();
        this.expandOpt = Optional.empty();
        this.direction = Optional.empty();
        this.params = Optional.empty();
    }

    // FfiExpandOpt
    private Optional<OpArg> expandOpt;

    // in/out/both
    private Optional<OpArg> direction;

    private Optional<QueryParams> params;

    public Optional<OpArg> getExpandOpt() {
        return expandOpt;
    }

    public Optional<OpArg> getDirection() {
        return direction;
    }

    public Optional<QueryParams> getParams() {
        return params;
    }

    public void setDirection(OpArg direction) {
        this.direction = Optional.of(direction);
    }

    public void setEdgeOpt(OpArg expandOpt) {
        this.expandOpt = Optional.of(expandOpt);
    }

    public void setParams(QueryParams params) {
        this.params = Optional.of(params);
    }
}
