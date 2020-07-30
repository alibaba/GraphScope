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
package com.alibaba.maxgraph.sdkcommon.manager;

import com.alibaba.maxgraph.sdkcommon.MaxGraphClient;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public abstract class ExecuteSchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteVertexSchemaManager.class);
    private MaxGraphClient client;
    StringBuilder executeBuilder;

    ExecuteSchemaManager(MaxGraphClient client) {
        this.client = client;
        this.executeBuilder = new StringBuilder();
    }

    public void execute() {
        String script = this.executeBuilder.toString();
        logger.info("execute schema manager with script=>" + script);
        try {
            Iterator<Result> result = this.client.executeQuery(script);
            if (result.hasNext()) {
                logger.info(result.next().getString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return this.executeBuilder.toString();
    }
}
