package com.alibaba.graphscope.parallel.netty.request;

import com.alibaba.graphscope.parallel.message.MessageStore;
import io.netty.buffer.ByteBuf;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class WritableRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> implements Writable,
    ImmutableClassesGiraphConfigurable<I, V, E> {

    public static final int UNKNOWN_SIZE = -1;

    /**
     * Configuration
     */
    protected ImmutableClassesGiraphConfiguration<I, V, E> conf;

    /**
     * Serialization of request type is taken care by encoder.
     *
     * @return request type.
     */
    public abstract RequestType getRequestType();

    public abstract void readFieldsRequest(DataInput input) throws IOException;

    public abstract void writeFieldsRequest(DataOutput output) throws IOException;

    public abstract int getNumBytes();

    /**
     * Apply this request on this message storage.
     *
     * @param messageStore message store.
     */
    public abstract void doRequest(MessageStore<I, Writable, ?> messageStore);

    @Override
    public final void readFields(DataInput input) throws IOException {
        readFieldsRequest(input);
    }

    @Override
    public final void write(DataOutput output) throws IOException {
        writeFieldsRequest(output);
    }

    /**
     *
     * @param buf
     */
    public abstract void setBuffer(ByteBuf buf);

    public abstract ByteBuf getBuffer();

    @Override
    public final ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
        return conf;
    }

    @Override
    public final void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        this.conf = conf;
    }

}
