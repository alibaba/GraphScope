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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.exception.DataException;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RpcProcessorUtils {

    public static Map<String, Object> deserializeProperty(byte[] data, GraphElement typeDef, GraphSchema schema) {
        DataInputStream read = new DataInputStream(new ByteArrayInputStream(data));
        List<GraphProperty> properties = typeDef.getPropertyList();
        Map<String, Object> kv = new HashMap<>(properties.size());
        try {
            int propCount = read.readInt();
            for (int i = 0; i < propCount; i++) {
                int pid = read.readInt();
                GraphProperty property = typeDef.getProperty(pid);
                kv.put(property.getName(), readValueByType(property.getDataType(), read));
            }
        } catch (EOFException e) {
            // ignore;
        } catch (IOException e) {
            throw DataException.unknowError(e);
        }


        return kv;
    }

    private static Object readValueByType(DataType dataType, DataInputStream read) throws IOException {
        switch (dataType) {
            case BOOL:
                return read.readByte();
            case CHAR:
                return read.readChar();
            case SHORT:
                return read.readShort();
            case INT:
                return read.readInt();
            case LONG:
                return read.readLong();
            case FLOAT:
                return read.readFloat();
            case DOUBLE:
                return read.readDouble();
            case STRING:
            case DATE: {
                int length = read.readInt();
                if (length <= 0) {
                    return "";
                }
                byte[] code = new byte[length];
                try {
                    read.readFully(code, 0, length);
                } catch (Exception e) {
                    throw new IOException("data broken: expect " + length + " bytes and readed code" + StringUtils.join(code, ","), e);
                }
                return new String(code, "UTF-8");
            }
            case BYTES: {
                int length = read.readInt();
                if (length <= 0) {
                    return "";
                }
                byte[] code = new byte[length];
                try {
                    read.readFully(code, 0, length);
                } catch (Exception e) {
                    throw new IOException("data broken: expect " + length + " bytes and readed code" + StringUtils.join(code, ","), e);
                }
                return code;
            }
            case INT_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.INT, read));
                }

                return valueList;
            }
            case LONG_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.LONG, read));
                }

                return valueList;
            }
            case STRING_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.STRING, read));
                }

                return valueList;
            }
            case FLOAT_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.FLOAT, read));
                }

                return valueList;
            }
            case DOUBLE_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.DOUBLE, read));
                }

                return valueList;
            }
            case BYTES_LIST: {
                int count = read.readInt();
                List valueList = Lists.newArrayList();
                for (int valIdx = 0; valIdx < count; valIdx++) {
                    valueList.add(readValueByType(DataType.BYTES, read));
                }

                return valueList;
            }
            default:
                throw new RuntimeException(dataType + "");
        }

    }
}
