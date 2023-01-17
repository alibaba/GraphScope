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
package com.alibaba.graphscope.function.test.config;

public class ServiceConfig {
    public static final Config<String> MANAGER_SERVER_URL =
            Config.stringConfig("manager.server.url", "default://");
    public static final Config<String> POD_HOSTS =
            Config.stringConfig("engine.pod.hosts", "default");
    public static final Config<String> CONTAINER_NAME =
            Config.stringConfig("engine.container.name", "default");
    public static final Config<String> ENGINE_PROPERTIES =
            Config.stringConfig("engine.properties", "timely.worker.per.process:2");
    public static final Config<String> LOAD_DATA_MODE =
            Config.stringConfig("load.data.mode", "k8s-exec");
    public static final Config<String> ENGINE_NS =
            Config.stringConfig("engine.namespace", "default");
    public static final Config<String> FUNCTION_TEST_MODE =
            Config.stringConfig("function.test.mode", "grape");
    public static final Config<String> CLIENT_SERVER_URL =
            Config.stringConfig("client.server.url", "default://");

    public static Config<String> LOAD_DATA_CMD(String graphName) {
        return Config.stringConfig(String.format("%s.load.data.cmd", graphName), "default");
    }
}
