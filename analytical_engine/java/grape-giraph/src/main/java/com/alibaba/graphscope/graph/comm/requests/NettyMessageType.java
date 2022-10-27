/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.graph.comm.requests;

public enum NettyMessageType {
    BYTE_ARRAY_MESSAGE(ByteArrayMessage.class),
    AGGREGATOR_MESSAGE(AggregatorMessage.class),
    NETTY_WRITABLE_MESSAGE(NettyWritableMessage.class);

    private Class<? extends NettyMessage> requestClass;

    private NettyMessageType(Class<? extends NettyMessage> clz) {
        this.requestClass = clz;
    }

    public Class<? extends NettyMessage> getRequestClass() {
        return requestClass;
    }
}
