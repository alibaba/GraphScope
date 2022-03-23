/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.parallel.netty.request.impl;

import com.alibaba.graphscope.parallel.message.DefaultMessageStore;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * A special type of request which store byteBuf as store.
 */
public class ByteBufRequest extends WritableRequest {

    private static Logger logger = LoggerFactory.getLogger(ByteBufRequest.class);

    private ByteBuf buf;

    public ByteBufRequest(ByteBuf in) {
        buf = in;
    }

    /**
     * Default constructor for reflection usage. SetBuffer should be called after construction.
     */
    public ByteBufRequest() {}

    /**
     * Serialization of request type is taken care by encoder.
     *
     * @return request type.
     */
    @Override
    public RequestType getRequestType() {
        return RequestType.BYTEBUF_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public int getNumBytes() {
        if (Objects.nonNull(buf)) {
            return buf.readableBytes();
        }
        return 0;
    }

    @Override
    public ByteBuf getBuffer() {
        return buf;
    }

    /**
     * @param buf
     */
    @Override
    public void setBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    /**
     * Apply this request on this message storage.
     *
     * @param messageStore message store.
     */
    @Override
    public void doRequest(MessageStore messageStore) {
        if (Objects.isNull(buf)) {
            throw new IllegalStateException("try to do request on an empty byteBuf request");
        }
        if (messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore1 = (DefaultMessageStore) messageStore;
            messageStore1.digestByteBuffer(buf, false);

            if (buf.readableBytes() != 0) {
                logger.error(
                        "Error: still bytes available, but not readable: {}", buf.readableBytes());
            }
        } else {
            throw new IllegalStateException("Not available mesageStore" + messageStore);
        }
    }

    @Override
    public String toString() {
        return "ByteBufRequest(size=" + buf.readableBytes() + ")";
    }
}
