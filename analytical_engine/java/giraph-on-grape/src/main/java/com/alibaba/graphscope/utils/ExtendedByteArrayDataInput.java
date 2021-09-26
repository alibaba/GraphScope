package com.alibaba.graphscope.utils;


import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Provides access to a internals of ByteArrayInputStream
 */
public class ExtendedByteArrayDataInput extends ByteArrayInputStream
    implements ExtendedDataInput {
    /** Internal data input */
    private final DataInput dataInput;
    /**
     * Constructor
     *
     * @param buf Buffer to read
     */
    public ExtendedByteArrayDataInput(byte[] buf) {
        super(buf);
        dataInput = new DataInputStream(this);
    }

    /**
     * Get access to portion of a byte array
     *
     * @param buf Byte array to access
     * @param offset Offset into the byte array
     * @param length Length to read
     */
    public ExtendedByteArrayDataInput(byte[] buf, int offset, int length) {
        super(buf, offset, length);
        dataInput = new DataInputStream(this);
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public boolean endOfInput() {
        return available() == 0;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        dataInput.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        dataInput.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return dataInput.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return dataInput.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return dataInput.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return dataInput.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return dataInput.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return dataInput.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return dataInput.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return dataInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return dataInput.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return dataInput.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return dataInput.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return dataInput.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return dataInput.readUTF();
    }
}

