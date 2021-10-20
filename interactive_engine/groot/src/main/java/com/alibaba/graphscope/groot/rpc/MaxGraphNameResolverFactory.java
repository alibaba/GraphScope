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
package com.alibaba.graphscope.groot.rpc;

import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import io.grpc.NameResolver;

import java.net.URI;

public class MaxGraphNameResolverFactory extends NameResolver.Factory {

    private NodeDiscovery discovery;

    public MaxGraphNameResolverFactory(NodeDiscovery discovery) {
        this.discovery = discovery;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        switch (targetUri.getScheme()) {
            case ChannelManager.SCHEME:
                return new NodeNameResolver(discovery, targetUri);
            default:
                return null;
        }
    }

    @Override
    public String getDefaultScheme() {
        return "unknown";
    }
}
