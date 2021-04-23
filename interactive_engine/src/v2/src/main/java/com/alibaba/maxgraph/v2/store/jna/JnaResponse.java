package com.alibaba.maxgraph.v2.store.jna;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.io.Closeable;
import java.io.IOException;


@Structure.FieldOrder({"success", "hasDdl", "errMsg", "data", "len"})
public class JnaResponse extends Structure implements Closeable {

    public int success;
    public int hasDdl;
    public String errMsg;
    public Pointer data;
    public int len;

    public boolean success() {
        return success == 1;
    }

    public boolean hasDdl() {
        return hasDdl == 1;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public byte[] getData() {
        if (this.data != null) {
            return this.data.getByteArray(0, this.len);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        setAutoSynch(false);
        GraphLibrary.INSTANCE.dropJnaResponse(this);
    }
}
