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
import com.alibaba.graphscope.groot.wal.LogWriter;
import com.alibaba.graphscope.groot.wal.kafka.KafkaLogService;

public class ReadOnlyLogService extends KafkaLogService {

    public ReadOnlyLogService(Configs configs) {
        super(configs);
    }

    @Override
    public LogWriter createWriter() {
        return new ReadOnlyLogWriter();
    }
}
