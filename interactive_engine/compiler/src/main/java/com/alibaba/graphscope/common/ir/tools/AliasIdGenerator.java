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

package com.alibaba.graphscope.common.ir.tools;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasIdGenerator {
    private final AtomicInteger idGenerator;
    private Map<String, Integer> aliasNameToIdMap;

    public AliasIdGenerator() {
        this.idGenerator = new AtomicInteger();
        this.aliasNameToIdMap = new HashMap<>();
    }

    public int generate(@Nullable String aliasName) {
        if (aliasName == null || aliasName == AliasInference.DEFAULT_NAME) {
            return AliasInference.DEFAULT_ID;
        }
        Integer aliasId = aliasNameToIdMap.get(aliasName);
        if (aliasId == null) {
            aliasId = idGenerator.getAndIncrement();
            aliasNameToIdMap.put(aliasName, aliasId);
        }
        return aliasId;
    }
}
