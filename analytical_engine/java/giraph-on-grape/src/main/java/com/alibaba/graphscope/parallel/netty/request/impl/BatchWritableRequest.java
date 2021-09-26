package com.alibaba.graphscope.parallel.netty.request.impl;

import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import com.alibaba.graphscope.utils.Gid2DataFixed;
import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchWritableRequest extends WritableRequest {

    private Gid2DataFixed data;
    private static Logger logger = LoggerFactory.getLogger(BatchWritableRequest.class);

    public BatchWritableRequest() {

    }

    public BatchWritableRequest(Gid2DataFixed data) {
        this.data = data;
    }

    /**
     * Serialization of request type is taken care by encoder.
     *
     * @return request type.
     */
    @Override
    public RequestType getRequestType() {
        return RequestType.BATCH_WRITABLE_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {
        int size = input.readInt();
        data = new Gid2DataFixed(size);
        for (int i = 0; i < size; ++i) {
            Writable inMsg = getConf().createInComingMessageValue();
            long gid = input.readLong();
            inMsg.readFields(input);
            data.add(gid, inMsg);
        }
    }

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {
        if (Objects.nonNull(this.data)) {
            this.data.write(output);
        } else {
            throw new IllegalStateException("Try to serialize an empty request");
        }
    }

    @Override
    public int getNumBytes() {
//        return data.serializedSize();
        return UNKNOWN_SIZE;
    }

    /**
     * @param buf
     */
    @Override
    public void setBuffer(ByteBuf buf) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public ByteBuf getBuffer() {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Apply this request on this message storage.
     *
     * @param messageStore message store.
     */
    @Override
    public void doRequest(MessageStore messageStore) {
        long[] gids = data.getGids();
        Writable[] msgOnVertex = data.getMsgOnVertex();
        for (int i = 0; i < data.size(); ++i) {
            messageStore.addGidMessage(gids[i], msgOnVertex[i]);
        }
    }

    @Override
    public String toString() {
        return "BatchWritableRequest(" + data + ")";
    }
}
