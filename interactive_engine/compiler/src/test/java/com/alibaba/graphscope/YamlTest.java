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

package com.alibaba.graphscope;

import com.alibaba.graphscope.common.config.*;

import org.junit.Assert;
import org.junit.Test;

public class YamlTest {
    @Test
    public void test() throws Exception {
        YamlConfigs configs = new YamlConfigs("./gs_interactive.yaml", FileLoadType.RESOURCES);
        Assert.assertEquals(
                "PlannerConfig{isOn=true, opt=RBO, rules=[FilterMatchRule]}",
                PlannerConfig.create(configs).toString());
        Assert.assertEquals(
                "localhost:8001, localhost:8005", PegasusConfig.PEGASUS_HOSTS.get(configs));
        Assert.assertEquals(3, (int) PegasusConfig.PEGASUS_WORKER_NUM.get(configs));
        Assert.assertEquals(2048, (int) PegasusConfig.PEGASUS_BATCH_SIZE.get(configs));
        Assert.assertEquals(18, (int) PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs));
        Assert.assertEquals(
                "/home/workspace/data/modern/plugins",
                GraphConfig.GRAPH_STORED_PROCEDURES.get(configs));
        Assert.assertEquals(
                "/home/workspace/data/modern/graph.yaml", GraphConfig.GRAPH_SCHEMA.get(configs));
        Assert.assertEquals("pegasus", FrontendConfig.ENGINE_TYPE.get(configs));
        Assert.assertEquals(false, FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs));
        Assert.assertEquals(8003, (int) FrontendConfig.GREMLIN_SERVER_PORT.get(configs));
        Assert.assertEquals(false, FrontendConfig.NEO4J_BOLT_SERVER_DISABLED.get(configs));
        Assert.assertEquals(8002, (int) FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(configs));
        Assert.assertEquals(200, (int) FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs));
    }
}
