/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.frontendservice.monitor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.math.DoubleMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MemoryMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryMonitor.class);
    private static final String TOTAL_MEMORY = "TOTAL_HEAP_MEMORY";
    private static final String FREE_MEMORY = "FREE_HEAP_MEMORY";
    private double memThreshold;
    private LoadingCache<String, Long> totalJvmMemoryInfo =
            CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.SECONDS).build(new CacheLoader<String, Long>() {
                @Override
                public Long load(String s) throws Exception {
                    return Runtime.getRuntime().totalMemory();
                }
            });

    private LoadingCache<String, Long> freeJvmMemoryInfo =
            CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.SECONDS).build(new CacheLoader<String, Long>() {
                @Override
                public Long load(String s) throws Exception {
                    return Runtime.getRuntime().freeMemory();
                }
            });

    public MemoryMonitor(double memThreshold) {
        this.memThreshold = memThreshold;
    }

    public boolean isMemorySafe() {
        // TODO: 整体memory监控
        try {
            return freeJvmMemoryInfo.get(FREE_MEMORY) >=
                    DoubleMath.roundToLong(totalJvmMemoryInfo.get(TOTAL_MEMORY) * (1d - memThreshold), RoundingMode.DOWN);
        } catch (ExecutionException e) {
            LOG.warn("{}", e);
            return false;
        }
    }
}
