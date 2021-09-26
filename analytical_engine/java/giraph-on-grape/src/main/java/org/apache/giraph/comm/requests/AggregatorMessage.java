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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AggregatorMessage extends ByteArrayMessage {

    /**
     * The position where this aggregator resides.
     */
    private String aggregatorId;

    private String value;

    public AggregatorMessage() {}

    public AggregatorMessage(String aggregatorId, String value, byte[] valueBytes) {
        this.aggregatorId = aggregatorId;
        this.value = value;
        this.setData(valueBytes);
    }

    public String getValue() {
        return value;
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize()
                + aggregatorId.getBytes(StandardCharsets.UTF_8).length
                + value.getBytes(StandardCharsets.UTF_8).length;
    }

    public String getAggregatorId() {
        return aggregatorId;
    }

    @Override
    public NettyMessageType getMessageType() {
        return NettyMessageType.AGGREGATOR_MESSAGE;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        aggregatorId = input.readUTF();
        value = input.readUTF();
        super.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(aggregatorId);
        output.writeUTF(value);
        super.write(output);
    }

    public String toString() {
        return "aggregatorMessage:id[" + aggregatorId + "], value" + value;
    }
}
