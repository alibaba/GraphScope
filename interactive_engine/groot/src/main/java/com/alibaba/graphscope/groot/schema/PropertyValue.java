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
import com.alibaba.maxgraph.proto.groot.PropertyValuePb;
import com.google.protobuf.ByteString;

import java.util.Arrays;
import java.util.Objects;

public class PropertyValue {
    private DataType dataType;
    private byte[] valBytes;
    private Object valObject;

    public PropertyValue(DataType dataType, byte[] valBytes) {
        this.dataType = dataType;
        this.valBytes = valBytes;
    }

    public PropertyValue(DataType dataType, Object valObject) {
        this.dataType = dataType;
        this.valBytes = SerdeUtils.objectToBytes(dataType, stringToObj(dataType, valObject));
        this.valObject = valObject;
    }

    private static Object stringToObj(DataType dataType, Object val) {
        String valString;
        if (val instanceof String) {
            valString = (String) val;
        } else {
            return val;
        }
        try {
            switch (dataType) {
                case BOOL:
                    return Boolean.parseBoolean(valString);
                case CHAR:
                    return valString.charAt(0);
                case SHORT:
                    return Short.valueOf(valString);
                case INT:
                    return Integer.valueOf(valString);
                case LONG:
                    return Long.valueOf(valString);
                case FLOAT:
                    return Float.valueOf(valString);
                case DOUBLE:
                    return Double.valueOf(valString);
                case STRING:
                    return valString;
                default:
                    throw new IllegalStateException("Unexpected value: " + dataType);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to parse object to bytes. DataType [" + dataType +
                    "], Object [" + valString + "], class [" + valString.getClass() + "]", e);
        }
    }

    public static PropertyValue parseProto(PropertyValuePb proto) {
        try {
            DataType dataType = DataType.parseProto(proto.getDataType());
            byte[] bytes = proto.getVal().toByteArray();
            return new PropertyValue(dataType, bytes);
        } catch (Exception e) {
            return null;
        }
    }

    public PropertyValuePb toProto() {
        return PropertyValuePb.newBuilder()
                .setDataType(dataType.toProto())
                .setVal(ByteString.copyFrom(valBytes))
                .build();
    }

    public Object getValue() {
        if (valObject != null) {
            return valObject;
        }
        this.valObject = SerdeUtils.bytesToObject(this.dataType, this.valBytes);
        return valObject;
    }

    public DataType getDataType() {
        return dataType;
    }

    public byte[] getValBytes() {
        return valBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyValue that = (PropertyValue) o;
        return dataType == that.dataType &&
                Arrays.equals(valBytes, that.valBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(dataType);
        result = 31 * result + Arrays.hashCode(valBytes);
        return result;
    }

    @Override
    public String toString() {
        return "PropertyValue{" +
                "dataType=" + dataType +
                ", valObject=" + valObject +
                ", valBytes=" + encodeHexString(valBytes) +
                '}';
    }

    private String encodeHexString(byte[] byteArray) {
        StringBuffer hexStringBuffer = new StringBuffer();
        for (int i = 0; i < byteArray.length; i++) {
            hexStringBuffer.append(byteToHex(byteArray[i]));
        }
        return hexStringBuffer.toString();
    }

    private String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);
        return new String(hexDigits);
    }
}
