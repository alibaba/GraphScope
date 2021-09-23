/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.schema.DataType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SerdeUtils {

    public static byte[] objectToBytes(DataType dataType, Object valObject) {
        try {
            switch (dataType) {
                case BOOL:
                    return new byte[]{(byte) ((Boolean) valObject ? 1 : 0)};
                case CHAR:
                    return ByteBuffer.allocate(Character.BYTES).putChar((Character) valObject).array();
                case SHORT:
                    return ByteBuffer.allocate(Short.BYTES).putShort(Short.valueOf(valObject.toString())).array();
                case INT:
                    return ByteBuffer.allocate(Integer.BYTES).putInt(Integer.valueOf(valObject.toString())).array();
                case LONG:
                    return ByteBuffer.allocate(Long.BYTES).putLong(Long.valueOf(valObject.toString())).array();
                case FLOAT:
                    return ByteBuffer.allocate(Float.BYTES).putFloat(Float.valueOf(valObject.toString())).array();
                case DOUBLE:
                    return ByteBuffer.allocate(Double.BYTES).putDouble(Double.valueOf(valObject.toString())).array();
                case STRING:
                    return ((String) valObject).getBytes(StandardCharsets.UTF_8);
                case BYTES:
                    return (byte[]) valObject;
                case INT_LIST:
                    List<Integer> intList = ((List<Object>) valObject).stream().map(o -> Integer.valueOf(o.toString()))
                            .collect(Collectors.toList());
                    return listToBytes(intList, (dos, e) -> {
                        try {
                            dos.writeInt(e);
                        } catch (IOException ex) {
                            throw new IllegalArgumentException("write to bytes failed", ex);
                        }
                        return null;
                    });
                case LONG_LIST:
                    List<Long> longList = ((List<Object>) valObject).stream().map(o -> Long.valueOf(o.toString()))
                            .collect(Collectors.toList());
                    return listToBytes(longList, (dos, e) -> {
                        try {
                            dos.writeLong(e);
                        } catch (IOException ex) {
                            throw new IllegalArgumentException("write to bytes failed", ex);
                        }
                        return null;
                    });
                case FLOAT_LIST:
                    List<Float> floatList = ((List<Object>) valObject).stream().map(o -> Float.valueOf(o.toString()))
                            .collect(Collectors.toList());
                    return listToBytes(floatList, (dos, e) -> {
                        try {
                            dos.writeFloat(e);
                        } catch (IOException ex) {
                            throw new IllegalArgumentException("write to bytes failed", ex);
                        }
                        return null;
                    });
                case DOUBLE_LIST:
                    List<Double> doubleList = ((List<Object>) valObject).stream().map(o -> Double.valueOf(o.toString()))
                            .collect(Collectors.toList());
                    return listToBytes(doubleList, (dos, e) -> {
                        try {
                            dos.writeDouble(e);
                        } catch (IOException ex) {
                            throw new IllegalArgumentException("write to bytes failed", ex);
                        }
                        return null;
                    });
                case STRING_LIST:
                    List<String> stringList = ((List<Object>) valObject).stream().map(o -> o.toString())
                            .collect(Collectors.toList());
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeInt(stringList.size());

                    int off = 0;
                    List<byte[]> bytesList = new ArrayList<>(stringList.size());
                    for (String str : stringList) {
                        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                        bytesList.add(bytes);
                        off += bytes.length;
                        dos.writeInt(off);
                    }
                    for (byte[] bytes : bytesList) {
                        dos.write(bytes);
                    }
                    return bos.toByteArray();
                default:
                    throw new IllegalStateException("Unexpected value: " + dataType);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to parse object to bytes. DataType [" + dataType +
                    "], Object [" + valObject + "], class [" + valObject.getClass() + "]", e);
        }
    }



    public static Object bytesToObject(DataType dataType, byte[] valBytes) {
        try {
            Object valObject;
            switch (dataType) {
                case BOOL:
                    valObject = valBytes[0] != (byte) 0;
                    break;
                case CHAR:
                    valObject = ByteBuffer.wrap(valBytes).getChar();
                    break;
                case SHORT:
                    valObject = ByteBuffer.wrap(valBytes).getShort();
                    break;
                case INT:
                    valObject = ByteBuffer.wrap(valBytes).getInt();
                    break;
                case LONG:
                    valObject = ByteBuffer.wrap(valBytes).getLong();
                    break;
                case FLOAT:
                    valObject = ByteBuffer.wrap(valBytes).getFloat();
                    break;
                case DOUBLE:
                    valObject = ByteBuffer.wrap(valBytes).getDouble();
                    break;
                case STRING:
                    valObject = new String(valBytes, StandardCharsets.UTF_8);
                    break;
                case BYTES:
                    valObject = valBytes;
                    break;
                case INT_LIST:
                    valObject = parseListVal(valBytes, dis -> {
                        try {
                            return dis.readInt();
                        } catch (IOException e) {
                            throw new IllegalArgumentException("parse val failed", e);
                        }
                    });
                    break;
                case LONG_LIST:
                    valObject = parseListVal(valBytes, dis -> {
                        try {
                            return dis.readLong();
                        } catch (IOException e) {
                            throw new IllegalArgumentException("parse val failed", e);
                        }
                    });
                    break;
                case FLOAT_LIST:
                    valObject = parseListVal(valBytes, dis -> {
                        try {
                            return dis.readFloat();
                        } catch (IOException e) {
                            throw new IllegalArgumentException("parse val failed", e);
                        }
                    });
                    break;
                case DOUBLE_LIST:
                    valObject = parseListVal(valBytes, dis -> {
                        try {
                            return dis.readDouble();
                        } catch (IOException e) {
                            throw new IllegalArgumentException("parse val failed", e);
                        }
                    });
                    break;
                case STRING_LIST:
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(valBytes));
                    int size = dis.readInt();
                    List<Integer> strAccumulatedLength = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        strAccumulatedLength.add(dis.readInt());
                    }
                    List<String> list = new ArrayList<>(size);
                    if (size != 0) {
                        int len = strAccumulatedLength.get(0);
                        list.add(new String(readBytes(dis, len)));
                        for (int i = 1; i < size; i++) {
                            len = strAccumulatedLength.get(i) - strAccumulatedLength.get(i - 1);
                            list.add(new String(readBytes(dis, len)));
                        }
                    }
                    valObject = list;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + dataType);
            }
            return valObject;
        } catch (IOException e) {
            throw new IllegalArgumentException("parse val failed", e);
        }
    }


    private static byte[] readBytes(DataInputStream dis, int len) throws IOException {
        byte[] bytes = new byte[len];
        dis.read(bytes);
        return bytes;
    }

    private static <T> List<T> parseListVal(byte[] val, Function<DataInputStream, T> func) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val));
        int size = dis.readInt();
        List<T> l = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            l.add(func.apply(dis));
        }
        return l;
    }

    private static <T> byte[] listToBytes(List<T> list, BiFunction<DataOutputStream, T, Void> f) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(list.size());
        for (T element : list) {
            f.apply(dos, element);
        }
        return bos.toByteArray();
    }

}
