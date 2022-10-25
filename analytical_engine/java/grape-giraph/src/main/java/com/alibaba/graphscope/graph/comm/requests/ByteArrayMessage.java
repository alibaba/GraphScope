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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class ByteArrayMessage implements NettyMessage {

    private byte[] data;

    public ByteArrayMessage() {}

    public ByteArrayMessage(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * Get request data in the form of {@link DataInput}
     *
     * @return Request data as {@link DataInput}
     */
    public DataInput getDataInput() {
        return new DataInputStream(new ByteArrayInputStream(data));
    }

    //    /**
    //     * Wraps the byte array with UnsafeByteArrayInputStream stream.
    //     * @return UnsafeByteArrayInputStream
    //     */
    //    public UnsafeByteArrayInputStream getUnsafeByteArrayInput() {
    //        return new UnsafeByteArrayInputStream(data);
    //    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int dataLength = input.readInt();
        data = new byte[dataLength];
        input.readFully(data);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(data.length);
        output.write(data);
    }

    @Override
    public int getSerializedSize() {
        return data.length + 4;
    }

    @Override
    public NettyMessageType getMessageType() {
        return null;
    }
}
