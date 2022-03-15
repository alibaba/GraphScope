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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * A wrapper to improve debugging. It passes the call() invocation to the provided callable, and
 * upon any exception logs the stacktrace and rethrows the exception. The logging functionality is
 * missing in FutureTask.
 *
 * @param <V> Return type of call()
 */
public class LogStacktraceCallable<V> implements Callable<V> {

    /**
     * Class logger
     */
    private static Logger logger = LoggerFactory.getLogger(LogStacktraceCallable.class);

    /**
     * Pass call() to this callable.
     */
    private final Callable<V> callable;
    /**
     * Uncaught exception handler, if any
     */
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    /**
     * Construct an instance that will pass call() to the given callable.
     *
     * @param callable Callable
     */
    public LogStacktraceCallable(Callable<V> callable) {
        this(callable, null);
    }

    /**
     * Construct an instance that will pass call() to the given callable.
     *
     * @param callable                 Callable
     * @param uncaughtExceptionHandler Uncaught exception handler, if any
     */
    public LogStacktraceCallable(
            Callable<V> callable, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.callable = callable;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public V call() throws Exception {
        try {
            return callable.call();
            // We catch, log stack trace of, and rethrow all exceptions. It's OK to
            // skip style check.
            // CHECKSTYLE: stop IllegalCatch
        } catch (Exception e) {
            // CHECKSTYLE: resume IllegalCatch
            logger.error("Execution of callable failed", e);
            if (uncaughtExceptionHandler != null) {
                uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
            }
            throw e;
        }
    }
}
