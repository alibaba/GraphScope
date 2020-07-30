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
package com.alibaba.maxgraph.sdkcommon.client;

import java.util.Objects;

import com.alibaba.maxgraph.proto.EndpointProto;

import com.alibaba.maxgraph.sdkcommon.Protoable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;

public class Endpoint implements Protoable<EndpointProto> {

    private String ip;
    /**
     * default rpc port
     */
    private int port;

    /**
     * gremlin server port, frontend only
     */
    private int gremlinServerPort;

    /**
     * timely's communication port, executor only
     */
    private int runtimePort = 0;

    /**
     * runtime control port to query or cancel running job,
     * also is async gremlin service port.
     * executor only.
     */
    private int runtimeCtrlAndAsyncPort;

    public Endpoint(String ip, int port, int gremlinServerPort, int runtimeCtrlPort) {
        this.ip = ip;
        this.port = port;
        this.gremlinServerPort = gremlinServerPort;
        this.runtimeCtrlAndAsyncPort = runtimeCtrlPort;
    }

    @JsonCreator
    public Endpoint(String ip, int port, int gremlinServerPort) {
        this.ip = ip;
        this.port = port;
        this.gremlinServerPort = gremlinServerPort;
        this.runtimeCtrlAndAsyncPort = 0;
    }

    public Endpoint(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.gremlinServerPort = 0;
        this.runtimeCtrlAndAsyncPort = 0;
    }

    public String getIp() {
        return ip;
    }

    public void updateIp(String ip) {
        if (StringUtils.isNotEmpty(ip)) {
            this.ip = ip;
        }
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getGremlinServerPort() {
        return gremlinServerPort;
    }

    public void setGremlinServerPort(int gremlinServerPort) {
        this.gremlinServerPort = gremlinServerPort;
    }

    public int getRuntimeCtrlAndAsyncPort() {
        return runtimeCtrlAndAsyncPort;
    }

    public void setRuntimePort(int port) {
        this.runtimePort = port;
    }

    public int getRuntimePort() {
        return this.runtimePort;
    }

    @Override
    public String toString() {
        return Joiner.on(":").join(ip, port, gremlinServerPort, runtimePort, runtimeCtrlAndAsyncPort);
    }

    public static Endpoint fromProto(EndpointProto proto) {
        return new Endpoint(proto.getHost(), proto.getPort(), proto.getGremlinServerPort(), proto.getRuntimCtrlAndAsyncPort());
    }

    @Override
    public void fromProto(byte[] data) throws InvalidProtocolBufferException {
        EndpointProto endpointProto = EndpointProto.parseFrom(data);
        this.ip = endpointProto.getHost();
        this.port = endpointProto.getPort();
        this.gremlinServerPort = endpointProto.getGremlinServerPort();
        this.runtimePort = endpointProto.getRuntimePort();
        this.runtimeCtrlAndAsyncPort = endpointProto.getRuntimCtrlAndAsyncPort();
    }

    public EndpointProto toProto() {
        EndpointProto.Builder builder = EndpointProto.newBuilder();
        builder.setHost(ip);
        builder.setPort(port);
        builder.setGremlinServerPort(gremlinServerPort);
        builder.setRuntimePort(runtimePort);
        builder.setRuntimCtrlAndAsyncPort(runtimeCtrlAndAsyncPort);
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Endpoint endpoint = (Endpoint)o;
        return port == endpoint.port &&
            gremlinServerPort == endpoint.gremlinServerPort &&
            runtimePort == endpoint.runtimePort &&
                runtimeCtrlAndAsyncPort == endpoint.runtimeCtrlAndAsyncPort &&
            Objects.equals(ip, endpoint.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, gremlinServerPort, runtimePort, runtimeCtrlAndAsyncPort);
    }
}
