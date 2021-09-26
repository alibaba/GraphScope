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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface for requests to implement
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public abstract class WritableRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> implements Writable,
    ImmutableClassesGiraphConfigurable<I, V, E> {
    /**
     * Value to use when size of the request in serialized form is not known
     * or too expensive to calculate
     */
    public static final int UNKNOWN_SIZE = -1;

    /** Configuration */
    protected ImmutableClassesGiraphConfiguration<I, V, E> conf;

//    public int getClientId() {
//        return clientId;
//    }
//
//    public void setClientId(int clientId) {
//        this.clientId = clientId;
//    }
//
//    public long getRequestId() {
//        return requestId;
//    }
//
//    public void setRequestId(long requestId) {
//        this.requestId = requestId;
//    }

    /**
     * Get the size of the request in serialized form. The number returned by
     * this function can't be less than the actual size - if the size can't be
     * calculated correctly return WritableRequest.UNKNOWN_SIZE.
     *
     * @return The size (in bytes) of serialized request,
     * or WritableRequest.UNKNOWN_SIZE if the size is not known
     * or too expensive to calculate.
     */
//    public int getSerializedSize() {
//        // 4 for clientId, 8 for requestId
//        return 4 + 8;
//    }

    /**
     * Get the type of the request
     *
     * @return Request type
     */
    public abstract NettyMessageType getType();

    /**
     * Serialize the request
     *
     * @param input Input to read fields from
     */
    abstract void readFieldsRequest(DataInput input) throws IOException;

    /**
     * Deserialize the request
     *
     * @param output Output to write the request to
     */
    abstract void writeRequest(DataOutput output) throws IOException;

    @Override
    public final ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
        return conf;
    }

    @Override
    public final void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        this.conf = conf;
    }

    @Override
    public final void readFields(DataInput input) throws IOException {
//        clientId = input.readInt();
//        requestId = input.readLong();
        readFieldsRequest(input);
    }

    @Override
    public final void write(DataOutput output) throws IOException {
//        output.writeInt(clientId);
//        output.writeLong(requestId);
        writeRequest(output);
    }
}
