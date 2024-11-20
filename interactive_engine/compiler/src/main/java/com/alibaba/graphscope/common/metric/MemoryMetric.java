/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.metric;

import com.google.common.collect.ImmutableMap;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

public class MemoryMetric implements Metric<Map> {
    @Override
    public Key getKey() {
        return KeyFactory.MEMORY;
    }

    @Override
    public Map getValue() {
        // jvm memory status
        long jvmFreeMem = Runtime.getRuntime().freeMemory();
        long jvmTotalMem = Runtime.getRuntime().totalMemory();
        long jvmUsedMem = jvmTotalMem - jvmFreeMem;

        // Direct memory
        List<BufferPoolMXBean> bufferPools =
                ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        long directUsedMem = ValueFactory.INVALID_LONG;
        long directTotalMem = ValueFactory.INVALID_LONG;
        for (BufferPoolMXBean bufferPool : bufferPools) {
            if ("direct".equalsIgnoreCase(bufferPool.getName())) {
                directUsedMem = bufferPool.getMemoryUsed();
                directTotalMem = bufferPool.getTotalCapacity();
            }
        }
        return ImmutableMap.of(
                "jvm.used",
                jvmUsedMem,
                "jvm.total",
                jvmTotalMem,
                "direct.used",
                directUsedMem,
                "direct.total",
                directTotalMem);
    }
}
