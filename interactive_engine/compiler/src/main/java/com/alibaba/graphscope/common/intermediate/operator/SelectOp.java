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

public class SelectOp extends InterOpBase {
    public SelectOp() {
        super();
        this.predicate = Optional.empty();
    }

    private Optional<OpArg> predicate;

    public Optional<OpArg> getPredicate() {
        return predicate;
    }

    public void setPredicate(OpArg predicate) {
        this.predicate = Optional.of(predicate);
    }
}
