package com.alibaba.graphscope.serialization;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class ByteBufTest {
    private ByteBuf buf;
    private ByteBufOutputStream bufOutputStream;

    @Before
    public void init(){
        buf = ByteBufAllocator.DEFAULT.buffer(8);
        bufOutputStream = new ByteBufOutputStream(buf);
    }

    @Test
    public void test() throws IOException {
        bufOutputStream.writeInt(0);
        bufOutputStream.writeInt(1);
        assert bufOutputStream.writtenBytes() == 8;
        assert buf.readableBytes() == 8;
        buf.clear();
        bufOutputStream.writeInt(2);
        bufOutputStream.writeInt(3);
        assert buf.getInt(0) == 2;
        assert buf.getInt(4) == 3;
    }
}
