/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.utils;

//import org.slf4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for thread related functions.
 */
public class ThreadUtils {

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(ThreadUtils.class);

    /**
     * Utility class. Do not inherit or create objects.
     */
    private ThreadUtils() {
    }

    /**
     * Creates new thread factory with specified thread name format.
     *
     * @param nameFormat       defines naming format for threads created by thread factory
     * @param exceptionHandler handles uncaught exceptions in all threads produced created thread
     *                         factory
     * @return new thread factory with specified thread name format and exception handler.
     */
    public static ThreadFactory createThreadFactory(
        String nameFormat,
        Thread.UncaughtExceptionHandler exceptionHandler) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder().
            setNameFormat(nameFormat).setDaemon(true);
        if (exceptionHandler != null) {
            builder.setUncaughtExceptionHandler(exceptionHandler);
        }
        return builder.build();
    }

    /**
     * Creates new thread factory with specified thread name format.
     *
     * @param nameFormat defines naming format for threads created by thread factory
     * @return new thread factory with specified thread name format
     */
    public static ThreadFactory createThreadFactory(String nameFormat) {
        return createThreadFactory(nameFormat, null);
    }

    /**
     * Submit a callable to executor service, ensuring any exceptions are caught with provided
     * exception handler.
     * <p>
     * When using submit(), UncaughtExceptionHandler which is set on ThreadFactory isn't used, so we
     * need this utility.
     *
     * @param executorService          Executor service to submit callable to
     * @param callable                 Callable to submit
     * @param uncaughtExceptionHandler Handler for uncaught exceptions in callable
     * @param <T>                      Type of callable result
     * @return Future for callable
     */
    public static <T> Future<T> submitToExecutor(
        ExecutorService executorService,
        Callable<T> callable,
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        return executorService.submit(
            new LogStacktraceCallable<>(callable, uncaughtExceptionHandler));
    }

    /**
     * Start thread with specified name and runnable, and make it daemon
     *
     * @param threadName Name of the thread
     * @param runnable   Runnable to execute
     * @return Thread
     */
    public static Thread startThread(Runnable runnable, String threadName) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Start thread with specified name, runnable and exception handler, and make it daemon
     *
     * @param runnable   Runnable to execute
     * @param threadName Name of the thread
     * @param handler    Exception handler
     * @return Thread
     */
    public static Thread startThread(Runnable runnable, String threadName,
        Thread.UncaughtExceptionHandler handler) {
        Thread thread = new Thread(runnable, threadName);
        thread.setUncaughtExceptionHandler(handler);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Sleep for specified milliseconds, logging and ignoring interrupted exceptions
     *
     * @param millis How long to sleep for
     * @return Whether the sleep was successful or the thread was interrupted
     */
    public static boolean trySleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            if (logger.isInfoEnabled()) {
                logger.info("Thread interrupted");
            }
            return false;
        }
    }
}
