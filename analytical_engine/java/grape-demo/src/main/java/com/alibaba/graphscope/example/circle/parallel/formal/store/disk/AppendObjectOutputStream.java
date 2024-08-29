package com.alibaba.graphscope.example.circle.parallel.formal.store.disk;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class AppendObjectOutputStream extends ObjectOutputStream {

    public AppendObjectOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    /**
     * 覆盖父类的方法,使其在已有对象信息并追加时,不写header信息
     * 查看源码会发现:writeStreamHeader方法会写入以下两行内容:
     * <p>
     * bout.writeShort(STREAM_MAGIC);
     * bout.writeShort(STREAM_VERSION);
     * <p>
     * 这两行对应的值:
     * final static short STREAM_MAGIC = (short)0xaced;
     * final static short STREAM_VERSION = 5;
     * <p>
     * 在文件中头部就会写入:AC ED 00 05
     * 一个文件对象只有在文件头出应该出现此信息,文件内容中不能出现此信息,否则会导致读取错误
     * 所以在追加时,就需要覆盖父类的writeStreamHeader方法,执行reset()方法
     * <p>
     * reset()方法写入的是这个:final static byte TC_RESET =        (byte)0x79;
     *
     * @throws IOException
     */
    @Override
    protected void writeStreamHeader() throws IOException {
        super.reset();
    }
}