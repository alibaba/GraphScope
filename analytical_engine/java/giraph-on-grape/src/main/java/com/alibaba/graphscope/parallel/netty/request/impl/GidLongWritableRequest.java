package com.alibaba.graphscope.parallel.netty.request.impl;

import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class GidLongWritableRequest extends WritableRequest {
    private long gid;
    private LongWritable writable;
    public GidLongWritableRequest(){
        writable =new LongWritable();
    }

    public GidLongWritableRequest(long gid, LongWritable writable){
        this.gid = gid;
        this.writable = writable;
    }

    public long getGid(){
        return this.gid;
    }
    public LongWritable getWritable(){
        return this.writable;
    }

    public long getValue(){
        return writable.get();
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.GID_Long_WRITABLE_REQUEST;
    }

    @Override
    public void readFieldsRequest(DataInput input) throws IOException {
        gid = input.readLong();
        writable.readFields(input);
    }

    @Override
    public void writeFieldsRequest(DataOutput output) throws IOException {
        output.writeLong(gid);
        writable.write(output);
    }

    /**
     * Should only return number of bytes in child class.
     * @return number of bytes
     */
    @Override
    public int getNumBytes() {
        return 16;
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
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public String toString(){
        return "GidLongWritableRequest(gid=" + gid + "writable=" + writable+")";
    }
}
