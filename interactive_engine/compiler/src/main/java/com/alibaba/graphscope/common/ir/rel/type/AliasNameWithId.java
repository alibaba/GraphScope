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

package com.alibaba.graphscope.common.ir.rel.type;

import com.alibaba.graphscope.common.ir.tools.AliasInference;

import java.util.Objects;

public class AliasNameWithId {
    public static final AliasNameWithId DEFAULT =
            new AliasNameWithId(AliasInference.DEFAULT_NAME, AliasInference.DEFAULT_ID);

    private final String aliasName;
    private final int aliasId;

    public AliasNameWithId(String aliasName, int aliasId) {
        this.aliasName = aliasName;
        this.aliasId = aliasId;
    }

    public String getAliasName() {
        return aliasName;
    }

    public int getAliasId() {
        return aliasId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AliasNameWithId that = (AliasNameWithId) o;
        return aliasId == that.aliasId && Objects.equals(aliasName, that.aliasName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliasName, aliasId);
    }
}
