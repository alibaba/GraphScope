/**
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
package com.compiler.demo.server.store;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static com.compiler.demo.server.store.StaticGraphStore.INVALID_ID;

public class GlobalIdMaker {
    public static final Pair<Long, Long> propertyIdRange = Pair.of(0L, Long.MAX_VALUE - 256L);
    public static final Pair<Long, Long> labelIdRange = Pair.of(0L, 255L);

    public GlobalIdMaker(List<Long> format) {
        // todo: dynamic generate id with format, such as [56L, 8L]
    }

    public long makeId(List<Long> ids) {
        long labelId = ids.get(0);
        long propertyId = ids.get(1);
        if (!isWithinRange(labelId, labelIdRange) || !isWithinRange(propertyId, propertyIdRange)) {
            return INVALID_ID;
        }
        return ((labelId << 56) | propertyId);
    }

    public static boolean isWithinRange(long value, Pair<Long, Long> range) {
        return value >= range.getLeft() && value <= range.getRight();
    }
}
