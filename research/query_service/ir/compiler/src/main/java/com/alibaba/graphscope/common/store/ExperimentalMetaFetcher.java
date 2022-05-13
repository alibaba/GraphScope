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

package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.gremlin.Utils;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ExperimentalMetaFetcher implements IrMetaFetcher {
    private static final Logger logger = LoggerFactory.getLogger(ExperimentalMetaFetcher.class);
    private Configs configs;
    private Map<String, Object> meta;

    public ExperimentalMetaFetcher(Configs configs) {
        this.configs = configs;
        init();
    }

    private void init() {
        String schemaFilePath = GraphConfig.GRAPH_SCHEMA.get(configs);
        try {
            String schema = Utils.readStringFromFile(schemaFilePath);
            this.meta = ImmutableMap.of(IrMetaFetcher.GRAPH_SCHEMA, schema);
        } catch (IOException e) {
            logger.info("open schema file {} fail", schemaFilePath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Map<String, Object>> fetch() {
        return Optional.ofNullable(this.meta);
    }
}
