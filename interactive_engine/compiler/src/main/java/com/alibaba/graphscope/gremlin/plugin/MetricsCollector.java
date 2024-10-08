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

package com.alibaba.graphscope.gremlin.plugin;

import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;

// collect metrics per gremlin query
public interface MetricsCollector {
    long getStartMillis();

    long getElapsedMillis();

    void stop();

    class Gremlin implements MetricsCollector {
        private final Timer.Context timeContext;
        private final long startMillis;
        private long elapsedMillis;

        public Gremlin(Timer timer) {
            this.timeContext = timer.time();
            this.startMillis = System.currentTimeMillis();
        }

        @Override
        public long getStartMillis() {
            return this.startMillis;
        }

        @Override
        public long getElapsedMillis() {
            return this.elapsedMillis;
        }

        @Override
        public void stop() {
            this.elapsedMillis = TimeUnit.NANOSECONDS.toMillis(timeContext.stop());
        }
    }

    class Cypher implements MetricsCollector {
        private final long startMills;
        private long endMills;

        public Cypher(long startMills) {
            this.startMills = startMills;
        }

        @Override
        public void stop() {
            this.endMills = System.currentTimeMillis();
        }

        @Override
        public long getStartMillis() {
            return this.startMills;
        }

        @Override
        public long getElapsedMillis() {
            return this.endMills - this.startMills;
        }
    }
}
