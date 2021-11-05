package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.io.Closeable;

@Structure.FieldOrder({"buffer", "len"})
public class FfiJobBuffer extends Structure implements Closeable{
    public FfiJobBuffer() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiJobBuffer implements Structure.ByValue {
    }

    public Pointer buffer;
    public int len;

    public byte[] getBytes() {
        if (buffer != null && len > 0) {
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; ++i) {
                bytes[i] = buffer.getByte(i);
            }
            return bytes;
        }
        return null;
    }

    @Override
    public void close() {
        setAutoSynch(false);
        IrCoreLibrary.INSTANCE.destroyJobBuffer(this);
    }
}
