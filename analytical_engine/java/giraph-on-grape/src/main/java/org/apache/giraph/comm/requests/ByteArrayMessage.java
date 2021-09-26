package org.apache.giraph.comm.requests;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class ByteArrayMessage implements NettyMessage {
    private byte[] data;

    public ByteArrayMessage(){

    }

    public ByteArrayMessage(byte []data){
        this.data = data;
    }

    public byte[] getData(){
        return data;
    }

    public void setData(byte[] data){
        this.data =data;
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
