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

package com.alibaba.graphscope.common.client.channel;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.HQPSConfig;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HostURIChannelFetcher implements ChannelFetcher<URI> {
    private Configs graphConfig;
    public HostURIChannelFetcher(Configs graphConfig) {
        this.graphConfig = graphConfig;
    }

    @Override
    public List<URI> fetch() {
        String hosts = HQPSConfig.HQPS_URIS.get(graphConfig);
        String[] hostsArr = hosts.split(",");
        return Arrays.asList(hostsArr).stream().map(k -> URI.create(k)).collect(Collectors.toList());
    }
}
