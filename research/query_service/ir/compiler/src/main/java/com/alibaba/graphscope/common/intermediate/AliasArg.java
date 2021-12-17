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

package com.alibaba.graphscope.common.intermediate;

import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.google.common.base.Objects;

public class AliasArg {
    private FfiNameOrId.ByValue alias;
    private boolean isQueryGiven;

    public AliasArg(FfiNameOrId.ByValue alias) {
        this.alias = alias;
        // set true by default
        this.isQueryGiven = true;
    }

    public AliasArg(FfiNameOrId.ByValue alias, boolean isQueryGiven) {
        this.alias = alias;
        this.isQueryGiven = isQueryGiven;
    }

    public FfiNameOrId.ByValue getAlias() {
        return alias;
    }

    public boolean isQueryGiven() {
        return isQueryGiven;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasArg aliasArg = (AliasArg) o;
        return isQueryGiven == aliasArg.isQueryGiven &&
                Objects.equal(alias, aliasArg.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(alias, isQueryGiven);
    }
}
