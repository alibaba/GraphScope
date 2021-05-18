package com.alibaba.maxgraph.dataload.databuild;

public class BytesRef {
    private byte[] array;
    private int offset;
    private int length;

    public BytesRef(byte[] array, int offset, int length) {
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getArray() {
        return array;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}
