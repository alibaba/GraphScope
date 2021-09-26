/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.comm.requests;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class NettyWritableMessage implements NettyMessage {

    private Writable data;
    private int repeatTimes;
    private int writableType;
    private String id;

    public NettyWritableMessage() {
        id = new String();
    }

    public NettyWritableMessage(Writable data, int repeatTimes, String id) {
        this.data = data;
        this.repeatTimes = repeatTimes;
        if (data instanceof LongWritable) {
            writableType = 1;
        } else {
            writableType = 0;
        }
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Writable getData() {
        return data;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.repeatTimes = input.readInt();
        this.writableType = input.readInt();
        this.id = input.readUTF();
        if (this.writableType == 1) {
            data = new LongWritable();
        } else {
            data = new IntWritable();
        }
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.readFields(input);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(repeatTimes);
        output.writeInt(writableType);
        output.writeUTF(id);
        for (int i = 0; i < repeatTimes; ++i) {
            this.data.write(output);
        }
    }

    @Override
    public int getSerializedSize() {
        // Caution.
        return 4 + 4 + repeatTimes * 8 + id.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public NettyMessageType getMessageType() {
        return NettyMessageType.NETTY_WRITABLE_MESSAGE;
    }

    @Override
    public String toString() {
        return "NettyWritable@"
                + id
                + ":"
                + repeatTimes
                + ",type: "
                + writableType
                + "data: "
                + data;
    }
}
