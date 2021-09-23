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
package com.alibaba.maxgraph.common.config;

public class IngestorConfig {
    public static final Config<Integer> INGESTOR_QUEUE_BUFFER_MAX_COUNT =
            Config.intConfig("ingsetor.queue.buffer.max.count", 128);

    public static final Config<Integer> INGESTOR_SENDER_BUFFER_MAX_COUNT =
            Config.intConfig("ingestor.sender.buffer.max.count", 128);

    public static final Config<Long> INGESTOR_CHECK_PROCESSOR_INTERVAL_MS =
            Config.longConfig("ingestor.check.processor.interval.ms", 3000L);
}
