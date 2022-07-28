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

public class ApplyOp extends InterOpBase {
    // InterOpCollection
    private Optional<OpArg> subOpCollection;

    // Integer
    private Optional<OpArg> subRootId;

    // FfiJoinKind
    private Optional<OpArg> joinKind;

    public ApplyOp() {
        super();
        subOpCollection = Optional.empty();
        subRootId = Optional.empty();
        joinKind = Optional.empty();
    }

    public Optional<OpArg> getSubOpCollection() {
        return subOpCollection;
    }

    public Optional<OpArg> getSubRootId() {
        return subRootId;
    }

    public Optional<OpArg> getJoinKind() {
        return joinKind;
    }

    public void setSubOpCollection(OpArg subOpCollection) {
        this.subOpCollection = Optional.of(subOpCollection);
    }

    public void setSubRootId(OpArg subRootId) {
        this.subRootId = Optional.of(subRootId);
    }

    public void setJoinKind(OpArg joinKind) {
        this.joinKind = Optional.of(joinKind);
    }
}
