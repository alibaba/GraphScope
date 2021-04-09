package com.alibaba.maxgraph.v2.common.rpc;

import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
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
