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

package com.alibaba.graphscope.gremlin.service;

import com.alibaba.graphscope.common.client.HostsChannelFetcher;
import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FileLoadType;
import com.alibaba.graphscope.common.store.ExperimentalMetaFetcher;
import com.alibaba.graphscope.common.store.IrMetaFetcher;

public class GraphServiceMain {
    public static void main(String[] args) throws Exception {
        Configs configs = new Configs("conf/ir.compiler.properties", FileLoadType.RELATIVE_PATH);
        IrMetaFetcher storeConfigs = new ExperimentalMetaFetcher(configs);
        RpcChannelFetcher fetcher = new HostsChannelFetcher(configs);

        IrGremlinServer server = new IrGremlinServer();
        server.start(configs, storeConfigs, fetcher);
    }
}
