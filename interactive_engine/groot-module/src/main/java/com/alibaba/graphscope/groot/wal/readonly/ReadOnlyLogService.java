/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.wal.readonly;

import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.KafkaConfig;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReadOnlyLogService implements LogService {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyLogService.class);
    private final String servers;
    private final String topic;

    public ReadOnlyLogService(Configs configs) {
        this.servers = KafkaConfig.KAFKA_SERVERS.get(configs);
        this.topic = KafkaConfig.KAKFA_TOPIC.get(configs);
        logger.info("Initialized MockLogService");
    }

    @Override
    public void init() {}

    @Override
    public void destroy() {}

    @Override
    public boolean initialized() {
        return true;
    }

    @Override
    public LogWriter createWriter(int queueId) {
        return new ReadOnlyLogWriter();
    }

    @Override
    public LogReader createReader(int queueId, long offset) throws IOException {
        return createReader(queueId, offset, -1);
    }

    @Override
    public LogReader createReader(int queueId, long offset, long timestamp) throws IOException {
        return new ReadOnlyLogReader(servers, topic, queueId);
    }

    @Override
    public void deleteBeforeOffset(int queueId, long offset) throws IOException {}
}
