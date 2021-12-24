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

package com.alibaba.graphscope.common.jna.type;

import com.google.common.base.Objects;
import com.sun.jna.Structure;

@Structure.FieldOrder({"alias", "isQueryGiven"})
public class FfiAlias extends Structure {
    public static class ByValue extends FfiAlias implements Structure.ByValue {
    }

    public FfiNameOrId.ByValue alias;
    public boolean isQueryGiven;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FfiAlias ffiAlias = (FfiAlias) o;
        return isQueryGiven == ffiAlias.isQueryGiven &&
                Objects.equal(alias, ffiAlias.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(alias, isQueryGiven);
    }
}