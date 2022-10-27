/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.utils;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.graphx.utils.DoubleDouble;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.serialization.FakeFFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIIntVector;
import com.alibaba.graphscope.stdcxx.FFIIntVectorFactory;
import com.alibaba.graphscope.stdcxx.FakeFFIByteVector;
import com.alibaba.graphscope.utils.array.PrimitiveArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class VertexDataUtils {

    private static Logger logger = LoggerFactory.getLogger(VertexDataUtils.class.getName());

    private static <VD> Tuple2<FFIByteVector, FFIIntVector> fillStringVertexArray(
            PrimitiveArray<VD> array, Class<? extends VD> clz) throws IOException {
        int size = array.size();
        FFIByteVectorOutputStream ffiByteVectorOutput = new FFIByteVectorOutputStream();
        FFIIntVector ffiOffset = (FFIIntVector) FFIIntVectorFactory.INSTANCE.create();
        ffiOffset.resize(size);
        ffiOffset.touch();
        long prevBytesWritten = 0;
        if (clz.equals(DoubleDouble.class)) {
            ffiByteVectorOutput.getVector().resize(size * 16L);
            ffiByteVectorOutput.getVector().touch();
            for (int i = 0; i < size; ++i) {
                DoubleDouble dd = (DoubleDouble) array.get(i);
                ffiByteVectorOutput.writeDouble(dd.a());
                ffiByteVectorOutput.writeDouble(dd.b());
                ffiOffset.set(i, (int) (ffiByteVectorOutput.bytesWriten() - prevBytesWritten));
                prevBytesWritten = ffiByteVectorOutput.bytesWriten();
            }
        } else {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(ffiByteVectorOutput);
            for (int i = 0; i < size; ++i) {
                objectOutputStream.writeObject(array.get(i));
                ffiOffset.set(i, (int) (ffiByteVectorOutput.bytesWriten() - prevBytesWritten));
                prevBytesWritten = ffiByteVectorOutput.bytesWriten();
            }
            objectOutputStream.flush();
        }

        ffiByteVectorOutput.finishSetting();
        long writenBytes = ffiByteVectorOutput.bytesWriten();
        logger.info(
                "write data array {} of type {}, writen bytes {}",
                size,
                clz.getName(),
                writenBytes);
        return new Tuple2<>(ffiByteVectorOutput.getVector(), ffiOffset);
    }

    private static <VD> ArrowArrayBuilder<VD> newArrowArrayBuilder(Class<? extends VD> clz) {
        if (clz.equals(Long.class) || clz.equals(long.class)) {
            ArrowArrayBuilder.Factory<VD> factory =
                    FFITypeFactory.getFactory(
                            ArrowArrayBuilder.class, "gs::ArrowArrayBuilder<int64_t>");
            return factory.create();
        } else if (clz.equals(Double.class) || clz.equals(double.class)) {
            ArrowArrayBuilder.Factory<VD> factory =
                    FFITypeFactory.getFactory(
                            ArrowArrayBuilder.class, "gs::ArrowArrayBuilder<double>");
            return factory.create();
        } else if (clz.equals(Integer.class) || clz.equals(int.class)) {
            ArrowArrayBuilder.Factory<VD> factory =
                    FFITypeFactory.getFactory(
                            ArrowArrayBuilder.class, "gs::ArrowArrayBuilder<int32_t>");
            return factory.create();
        } else {
            throw new IllegalStateException("Not recognized " + clz.getName());
        }
    }

    public static <T> T[] readComplexArray(StringTypedArray oldArray, Class<? extends T> clz)
            throws IOException, ClassNotFoundException {
        FakeFFIByteVector vector =
                new FakeFFIByteVector(oldArray.getRawData(), oldArray.getRawDataLength());
        FakeFFIByteVectorInputStream ffiInput = new FakeFFIByteVectorInputStream(vector);
        long len = oldArray.getLength();
        logger.info("reading {} objects from array of bytes {}", len, oldArray.getLength());

        if (clz.equals(DoubleDouble.class)) {
            T[] newArray = (T[]) new DoubleDouble[(int) len];
            for (int i = 0; i < len; ++i) {
                double a = ffiInput.readDouble();
                double b = ffiInput.readDouble();
                newArray[i] = (T) new DoubleDouble(a, b);
            }
            return newArray;
        } else {
            T[] newArray = (T[]) new Object[(int) len];
            ObjectInputStream objectInputStream = new ObjectInputStream(ffiInput);
            for (int i = 0; i < len; ++i) {
                T obj = (T) objectInputStream.readObject();
                newArray[i] = obj;
            }
            return newArray;
        }
    }
}
