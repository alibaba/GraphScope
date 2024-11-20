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

import java.util.Map;

public interface Metric<Value> {

    class KeyFactory {
        public static final Key MEMORY = new Key("memory");
        public static final Key RPC_CHANNELS_EXECUTOR_QUEUE =
                new Key("rpc.channels.executor.queue");
        public static final Key GREMLIN_EXECUTOR_QUEUE = new Key("gremlin.executor.queue");
        public static final Key GREMLIN_QPS = new Key("gremlin.qps");
    }

    class ValueFactory {
        public static long INVALID_LONG = -1l;
        public static int INVALID_INT = -1;
        public static Map INVALID_MAP = ImmutableMap.of();
    }

    Key getKey();

    Value getValue();

    class Key {
        private final String keyName;

        private Key(String keyName) {
            this.keyName = keyName;
        }

        @Override
        public String toString() {
            return this.keyName;
        }
    }
}
