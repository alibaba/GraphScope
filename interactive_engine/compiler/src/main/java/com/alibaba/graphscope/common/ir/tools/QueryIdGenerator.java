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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;

import java.util.concurrent.atomic.AtomicLong;

public class QueryIdGenerator {
    private final Configs configs;
    private final AtomicLong idGenerator;

    public QueryIdGenerator(Configs configs) {
        this.configs = configs;
        this.idGenerator = new AtomicLong(FrontendConfig.FRONTEND_SERVER_ID.get(configs));
    }

    public long generateId() {
        long delta = FrontendConfig.FRONTEND_SERVER_NUM.get(configs);
        return idGenerator.getAndAdd(delta);
    }

    public String generateName(long uniqueId) {
        return "ir_plan_" + uniqueId;
    }
}
