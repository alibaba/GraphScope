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

import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

public class NodeNameResolver extends NameResolver implements NodeDiscovery.Listener {
    private static final Logger logger = LoggerFactory.getLogger(NodeNameResolver.class);

    private NodeDiscovery discovery;
    private URI uri;
    private RoleType roleType;
    private int idx;

    private Listener2 listener;

    public NodeNameResolver(NodeDiscovery discovery, URI uri) {
        this.discovery = discovery;
        this.uri = uri;
        this.roleType = RoleType.fromName(uri.getAuthority());
        this.idx = Integer.valueOf(uri.getPath().substring(1));
    }

    @Override
    public String getServiceAuthority() {
        return uri.getAuthority();
    }

    @Override
    public void start(Listener2 listener) {
        logger.info("starting resolver for role [" + roleType + "] #[" + idx + "]");
        this.listener = listener;
        this.discovery.addListener(this);
    }

    @Override
    public void shutdown() {
        this.discovery.removeListener(this);
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        logger.debug(
                "Add nodes "
                        + nodes
                        + " for role "
                        + role
                        + " with current role type "
                        + this.roleType);
        if (role != this.roleType) {
            return;
        }
        MaxGraphNode maxGraphNode = nodes.get(this.idx);
        if (maxGraphNode == null) {
            return;
        }
        logger.info("connection ready. node [" + maxGraphNode + "]");
        InetSocketAddress address =
                new InetSocketAddress(maxGraphNode.getHost(), maxGraphNode.getPort());
        EquivalentAddressGroup server = new EquivalentAddressGroup(address);
        ResolutionResult resolutionResult =
                ResolutionResult.newBuilder()
                        .setAddresses(Collections.singletonList(server))
                        .setAttributes(Attributes.EMPTY)
                        .build();
        this.listener.onResult(resolutionResult);
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == this.roleType) {
            MaxGraphNode lostNode = nodes.get(this.idx);
            if (lostNode != null) {
                logger.info("connection lost. node [" + lostNode + "]");
                this.listener.onResult(ResolutionResult.newBuilder().build());
            }
        }
    }
}
