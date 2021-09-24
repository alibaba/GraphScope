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
package com.alibaba.maxgraph.common.util;

import org.slf4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryUtils {

    public static ThreadFactory daemonThreadFactoryWithLogExceptionHandler(String namePrefix, Logger logger) {
        AtomicInteger threadNumber = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        ThreadGroup group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        return r -> {
            Thread thread = new Thread(group, r, namePrefix + "[#" + threadNumber.getAndIncrement() + "]", 0);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> {
                logger.error("uncaught exception in [" + namePrefix + "], thread [" + t.getName() + "]", e);
            });
            return thread;
        };
    }

}
