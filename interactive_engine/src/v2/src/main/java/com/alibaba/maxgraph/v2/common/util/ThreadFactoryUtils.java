package com.alibaba.maxgraph.v2.common.util;

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
