package com.alibaba.graphscope.parallel.netty.request.impl;

import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

public class OneLongWritableRequest extends WritableRequest {

    private LongWritable data;
    public LongWritable getData(){
        return data;
    }

    public long getRawValue(){
        return data.get();
    }

    public void setData(LongWritable value){
        data = value;
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.ONE_LONG_WRITABLE_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {

    }

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {

    }

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
}
