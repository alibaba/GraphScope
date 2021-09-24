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

import com.alibaba.maxgraph.common.RoleType;

public class DiscoveryConfig {
    public static final String DNS_NAME_PREFIX_FORMAT = "dns.name.prefix.%s";

    public static final Config<String> DNS_NAME_PREFIX_FRONTEND =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.FRONTEND.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_INGESTOR =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.INGESTOR.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_COORDINATOR =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.COORDINATOR.getName()), "");

    public static final Config<String> DNS_NAME_PREFIX_STORE =
            Config.stringConfig(String.format(DNS_NAME_PREFIX_FORMAT, RoleType.STORE.getName()), "");
}
