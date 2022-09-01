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

import com.alibaba.graphscope.common.jna.type.PathOpt;
import com.alibaba.graphscope.common.jna.type.ResultOpt;

import java.util.Optional;

public class PathExpandOp extends ExpandOp {
    private Optional<OpArg> lower;

    private Optional<OpArg> upper;

    private PathOpt pathOpt;

    private ResultOpt resultOpt;

    public PathExpandOp() {
        super();
        this.lower = Optional.empty();
        this.upper = Optional.empty();
        this.pathOpt = PathOpt.Arbitrary;
        this.resultOpt = ResultOpt.EndV;
    }

    public PathExpandOp(ExpandOp other) {
        this();
        if (other.getAlias().isPresent()) {
            setAlias(other.getAlias().get());
        }
        if (other.getExpandOpt().isPresent()) {
            setEdgeOpt(other.getExpandOpt().get());
        }
        if (other.getDirection().isPresent()) {
            setDirection(other.getDirection().get());
        }
        if (other.getParams().isPresent()) {
            setParams(other.getParams().get());
        }
    }

    public Optional<OpArg> getLower() {
        return lower;
    }

    public Optional<OpArg> getUpper() {
        return upper;
    }

    public void setLower(OpArg lower) {
        this.lower = Optional.of(lower);
    }

    public void setUpper(OpArg upper) {
        this.upper = Optional.of(upper);
    }

    public PathOpt getPathOpt() {
        return pathOpt;
    }

    public ResultOpt getResultOpt() {
        return resultOpt;
    }

    public void setPathOpt(PathOpt pathOpt) {
        this.pathOpt = pathOpt;
    }

    public void setResultOpt(ResultOpt resultOpt) {
        this.resultOpt = resultOpt;
    }
}
