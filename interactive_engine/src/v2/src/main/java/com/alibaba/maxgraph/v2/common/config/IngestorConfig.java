package com.alibaba.maxgraph.v2.common.config;

public class IngestorConfig {
    public static final Config<Integer> INGESTOR_QUEUE_BUFFER_MAX_COUNT =
            Config.intConfig("ingsetor.queue.buffer.max.count", 128);

    public static final Config<Integer> INGESTOR_SENDER_BUFFER_MAX_COUNT =
            Config.intConfig("ingestor.sender.buffer.max.count", 128);

    public static final Config<Long> INGESTOR_CHECK_PROCESSOR_INTERVAL_MS =
            Config.longConfig("ingestor.check.processor.interval.ms", 3000L);
}
