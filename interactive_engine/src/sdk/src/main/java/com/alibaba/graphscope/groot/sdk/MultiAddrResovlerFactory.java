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
package com.alibaba.graphscope.groot.sdk;

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
        this.addrs = addrs.stream().map(EquivalentAddressGroup::new).collect(Collectors.toList());
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new NameResolver() {
            @Override
            public String getServiceAuthority() {
                return "none";
            }

            public void start(Listener2 listener) {
                listener.onResult(
                        ResolutionResult.newBuilder()
                                .setAddresses(addrs)
                                .setAttributes(Attributes.EMPTY)
                                .build());
            }

            @Override
            public void shutdown() {}
        };
    }

    @Override
    public String getDefaultScheme() {
        return "null";
    }
}
