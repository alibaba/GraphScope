/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.serialization;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.scijava.nativelib.NativeLoader;

import java.io.IOException;

public class FFIByteVectorStreamTest {

    static {
        try {
            NativeLoader.loadLibrary("grape-jni");
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        System.out.println(
                "str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
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
        System.out.println(
                "str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
        // Make sure we compare with same encoding
        Assert.assertTrue(str.equals(res));
    }

    @Test
    public void testDigestVector() throws IOException {
        FFIByteVector vector1 = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        FFIByteVector vector2 = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();

        vector1.resize(40);
        vector2.resize(40);
        for (int i = 0; i < 10; ++i) {
            vector1.setRawInt(i * 4, i);
            vector2.setRawInt(i * 4, i + 10);
        }
        FFIByteVectorInputStream inputStream2 = new FFIByteVectorInputStream();
        inputStream2.digestVector(vector1);
        inputStream2.digestVector(vector2);

        Assert.assertTrue(inputStream2.longAvailable() == 80);
        for (int i = 0; i < 20; ++i) {
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
