package com.alibaba.maxgraph.v2.sdk;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;

import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public class MultiAddrResovlerFactory extends NameResolver.Factory {

    private List<EquivalentAddressGroup> addrs;

    public MultiAddrResovlerFactory(List<SocketAddress> addrs) {
        this.addrs = addrs.stream()
                .map(EquivalentAddressGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new NameResolver() {
            @Override
            public String getServiceAuthority() {
                return "none";
            }

            public void start(Listener2 listener) {
                listener.onResult(ResolutionResult.newBuilder()
                .setAddresses(addrs)
                .setAttributes(Attributes.EMPTY)
                .build());
            }

            @Override
            public void shutdown() {

            }
        };
    }

    @Override
    public String getDefaultScheme() {
        return "null";
    }
}
