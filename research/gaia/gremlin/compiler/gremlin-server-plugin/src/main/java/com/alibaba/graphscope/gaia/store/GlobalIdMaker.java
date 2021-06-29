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
package com.alibaba.graphscope.gaia.store;

import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class GlobalIdMaker implements IdMaker<Long, List<Long>> {
    public static final Pair<Long, Long> propertyIdRange = Pair.of(0L, 0x00ffffffffffffffL);
    public static final Pair<Long, Long> labelIdRange = Pair.of(0L, 255L);

    public Long getId(List<Long> ids) {
        long labelId = ids.get(0);
        long propertyId = ids.get(1);
        if (!isWithinRange(labelId, labelIdRange)) {
            throw new RuntimeException("label id " + labelId + " out of range");
        }
        if (!isWithinRange(propertyId, propertyIdRange)) {
            throw new RuntimeException("property id " + propertyId + " out of range");
        }
        return ((labelId << 56) | propertyId);
    }

    public static boolean isWithinRange(long value, Pair<Long, Long> range) {
        return value >= range.getLeft() && value <= range.getRight();
    }
}
