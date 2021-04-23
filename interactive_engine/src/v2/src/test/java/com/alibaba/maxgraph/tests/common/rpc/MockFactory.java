package com.alibaba.maxgraph.tests.common.rpc;

import io.grpc.NameResolver;

import java.net.URI;

public class MockFactory extends NameResolver.Factory {
    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new NameResolver() {
            @Override
            public String getServiceAuthority() {
                return targetUri.getAuthority();
            }

            @Override
            public void shutdown() {
            }
        };
    }

    @Override
    public String getDefaultScheme() {
        return "unknown";
    }
}
