package com.alibaba.graphscope.serialization;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FFIByteVectorStreamTest {

    static {
        System.loadLibrary("giraph-jni");
    }

    private FFIByteVectorOutputStream outputStream;
    private FFIByteVectorInputStream inputStream;
    private long SIZE = 100;

    @Before
    public void prepare() {
        outputStream = new FFIByteVectorOutputStream();
        outputStream.resize(SIZE);
        inputStream = new FFIByteVectorInputStream();
    }

    @Test
    public void testInt() throws IOException {
        outputStream.reset();
        for (int i = 0; i < 25; ++i) {
            outputStream.writeInt(i);
        }
        FFIByteVector vector = outputStream.getVector();

        inputStream.setVector(vector);
        for (int i = 0; i < 25; ++i) {
            Assert.assertTrue(inputStream.readInt() == i);
        }
    }

    @Test
    public void testStringChar() throws IOException {
        String str = "123456";
        outputStream.reset();
        outputStream.writeChars(str);
        FFIByteVector vector = outputStream.getVector();
        inputStream.setVector(vector);

        char[] chars = new char[str.length()];
        inputStream.read(chars, 0, chars.length);
        String res = new String(chars);
        System.out
            .println("str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
        Assert.assertTrue(str.equals(res));
    }
    @Test
    public void testStringUTF() throws IOException {
        String str = "123456";
        outputStream.reset();
        outputStream.writeUTF(str);
        FFIByteVector vector = outputStream.getVector();
        inputStream.setVector(vector);

        String res = inputStream.readUTF();
        System.out
            .println("str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
        //Make sure we compare with same encoding
        Assert.assertTrue(str.equals(res));
    }

    @Test
    public void testDigestVector() throws IOException {
        FFIByteVector vector1 = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        FFIByteVector vector2 = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();

        vector1.resize(40);
        vector2.resize(40);
        for (int i = 0; i < 10; ++i){
            vector1.setRawInt(i * 4, i);
            vector2.setRawInt(i * 4, i + 10);
        }
        FFIByteVectorInputStream inputStream2 = new FFIByteVectorInputStream();
        inputStream2.digestVector(vector1);
        inputStream2.digestVector(vector2);

        Assert.assertTrue(inputStream2.longAvailable() ==80);
        for (int i = 0; i < 20; ++i){
            Assert.assertTrue(inputStream2.readInt() == i);
        }
    }

    @Test
    public void testOutPutStream() throws IOException {
        outputStream.reset();
        for (int i = 0; i < 25; ++i) {
            outputStream.writeInt(i);
        }
        Assert.assertTrue(outputStream.bytesWriten() == 100);
        FFIByteVector vector = outputStream.getVector();
        System.out.println("Buffer size: " + vector.size() + ", size: " + vector.size);
        outputStream.finishSetting();
        vector = outputStream.getVector();
        Assert.assertTrue(vector.size == outputStream.bytesWriten());
    }
}
