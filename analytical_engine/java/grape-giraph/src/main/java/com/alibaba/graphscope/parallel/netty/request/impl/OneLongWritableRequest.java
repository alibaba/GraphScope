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

import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;

import io.netty.buffer.ByteBuf;

import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OneLongWritableRequest extends WritableRequest {

    private LongWritable data;

    public LongWritable getData() {
        return data;
    }

    public void setData(LongWritable value) {
        data = value;
    }

    public long getRawValue() {
        return data.get();
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.ONE_LONG_WRITABLE_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {}

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {}

    @Override
    public int getNumBytes() {
        return 8;
    }

    /**
     * Apply this request on this message storage.
     *
     * @param messageStore message store.
     */
    @Override
    public void doRequest(MessageStore messageStore) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public ByteBuf getBuffer() {
        throw new IllegalStateException("not implemented");
    }

    /**
     * @param buf
     */
    @Override
    public void setBuffer(ByteBuf buf) {
        throw new IllegalStateException("Not implemented");
    }
}
