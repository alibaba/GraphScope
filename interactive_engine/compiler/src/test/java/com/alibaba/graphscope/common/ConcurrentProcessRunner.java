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

package com.alibaba.graphscope.common;

import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerScheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A JUnit runner that runs tests concurrently, the default thread pool size is 4.
 */
public class ConcurrentProcessRunner extends GremlinProcessRunner {
    private final ExecutorService executorService;

    public ConcurrentProcessRunner(Class<?> klass) throws InitializationError {
        super(klass);
        this.executorService = Executors.newFixedThreadPool(4);
        RunnerScheduler scheduler =
                new RunnerScheduler() {
                    @Override
                    public void schedule(Runnable childStatement) {
                        executorService.submit(childStatement);
                    }

                    @Override
                    public void finished() {
                        try {
                            executorService.shutdown();
                            executorService.awaitTermination(120, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        setScheduler(scheduler);
    }
}
