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

public class PathExpandOp extends ExpandOp {
    private Optional<OpArg> lower;

    private Optional<OpArg> upper;

    public PathExpandOp() {
        super();
        lower = Optional.empty();
        upper = Optional.empty();
    }

    public PathExpandOp(ExpandOp other) {
        this();
        if (other.getAlias().isPresent()) {
            setAlias(other.getAlias().get());
        }
        if (other.getIsEdge().isPresent()) {
            setEdgeOpt(other.getIsEdge().get());
        }
        if (other.getDirection().isPresent()) {
            setDirection(other.getDirection().get());
        }
        if (other.getLabels().isPresent()) {
            setLabels(other.getLabels().get());
        }
        if (other.getPredicate().isPresent()) {
            setPredicate(other.getPredicate().get());
        }
        if (other.getProperties().isPresent()) {
            setProperties(other.getProperties().get());
        }
        if (other.getLimit().isPresent()) {
            setLimit(other.getLimit().get());
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
}
