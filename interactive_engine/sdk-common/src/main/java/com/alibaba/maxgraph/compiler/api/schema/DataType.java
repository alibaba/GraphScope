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
package com.alibaba.maxgraph.compiler.api.schema;

import com.alibaba.maxgraph.proto.DataTypePb;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;

public enum DataType {
    UNKNOWN(0),
    BOOL(1),
    CHAR(2),
    SHORT(3),
    INT(4),
    LONG(5),
    FLOAT(6),
    DOUBLE(7),
    STRING(8),
    BYTES(9),
    INT_LIST(10),
    LONG_LIST(11),
    FLOAT_LIST(12),
    DOUBLE_LIST(13),
    STRING_LIST(14),
    BYTES_LIST(15),
    DATE(16);

    private final byte type;

    DataType(int type) {
        this.type = (byte) type;
    }

    private static final DataType[] TYPES = DataType.values();

    public static DataType fromId(byte id) {
        if (id < 0 || id >= TYPES.length) {
            throw new IllegalArgumentException("Unknown DataType: [" + id + "]");
        }
        return TYPES[id];
    }

    public static DataType parseProto(DataTypePb pb) {
        return fromId((byte) pb.getNumber());
    }

    public static DataType parseString(String type) {
        String upperType = StringUtils.upperCase(type);
        if (StringUtils.startsWith(upperType, "LIST<")) {
            String typeValue = StringUtils.removeEnd(StringUtils.removeStart(upperType, "LIST<"), ">") + "_LIST";
            return DataType.valueOf(typeValue);
        } else {
            return DataType.valueOf(type);
        }
    }

    public DataTypePb toProto() {
        return DataTypePb.forNumber(type);
    }

    @Override
    public String toString() {
        String dataTypeString = this.name();
        if (StringUtils.endsWith(dataTypeString, "_LIST")) {
            String[] dataTypeArray = StringUtils.splitByWholeSeparator(dataTypeString, "_");
            dataTypeString = "LIST<" + dataTypeArray[0] + ">";
        }

        return dataTypeString;
    }

    public boolean isFixedLength() {
        switch (this) {
            case BOOL:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public int getTypeLength() {
        switch (this) {
            case BOOL:
                return 1;
            case CHAR:
                return 2;
            case SHORT:
                return 2;
            case INT:
                return 4;
            case LONG:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            default:
                throw new UnsupportedOperationException("not a fixed length type [" + this + "]");
        }
    }

    public static DataType parseFromDataType(com.alibaba.maxgraph.sdkcommon.meta.DataType dataType) {
        InternalDataType internalDataType = dataType.getType();
        switch (internalDataType) {
            case BOOL: {
                return DataType.BOOL;
            }
            case CHAR: {
                return DataType.CHAR;
            }
            case SHORT: {
                return DataType.SHORT;
            }
            case INT: {
                return DataType.INT;
            }
            case LONG: {
                return DataType.LONG;
            }
            case FLOAT: {
                return DataType.FLOAT;
            }
            case DOUBLE: {
                return DataType.DOUBLE;
            }
            case STRING: {
                return DataType.STRING;
            }
            case BYTES: {
                return DataType.BYTES;
            }
            case DATE: {
                return DataType.STRING;
            }
            case LIST: {
                switch (InternalDataType.valueOf(dataType.getExpression())) {
                    case INT: {
                        return DataType.INT_LIST;
                    }
                    case LONG: {
                        return DataType.LONG_LIST;
                    }
                    case FLOAT: {
                        return DataType.FLOAT_LIST;
                    }
                    case DOUBLE: {
                        return DataType.DOUBLE_LIST;
                    }
                    case STRING: {
                        return DataType.STRING_LIST;
                    }
                    default: {
                        throw new IllegalArgumentException("Unsupport property data type " + dataType.toString());
                    }
                }
            }
            default: {
                throw new IllegalArgumentException("Unsupport property data type " + dataType.toString());
            }
        }
    }

    public Object getRandomValue() {
        switch (this) {
            case BOOL:
                return RandomUtils.nextBoolean();
            case CHAR:
                return (char) Math.abs(RandomUtils.nextInt()) % 127;
            case DATE:
                return new Date().toString();
            case SHORT:
                return (short) RandomUtils.nextInt();
            case INT:
                return RandomUtils.nextInt();
            case LONG:
                return RandomUtils.nextLong();
            case FLOAT:
                return RandomUtils.nextFloat();
            case DOUBLE:
                return RandomUtils.nextDouble();
            case STRING:
                return RandomStringUtils.randomAlphanumeric(64);
            case BYTES:
                return RandomStringUtils.random(64).getBytes();
            case INT_LIST:
                return Lists.newArrayList(RandomUtils.nextInt(), RandomUtils.nextInt(), RandomUtils.nextInt());
            case LONG_LIST:
                return Lists.newArrayList(RandomUtils.nextLong(), RandomUtils.nextLong(), RandomUtils.nextLong());
            case FLOAT_LIST:
                return Lists.newArrayList(RandomUtils.nextFloat(), RandomUtils.nextFloat(), RandomUtils.nextFloat());
            case DOUBLE_LIST:
                return Lists.newArrayList(RandomUtils.nextDouble(), RandomUtils.nextDouble(), RandomUtils.nextDouble());
            case STRING_LIST:
                return Lists.newArrayList(RandomStringUtils.randomAlphanumeric(64),
                        RandomStringUtils.randomAlphanumeric(64),
                        RandomStringUtils.randomAlphanumeric(64));
            case BYTES_LIST:
                return Lists.newArrayList(RandomStringUtils.randomAlphanumeric(64).getBytes(),
                        RandomStringUtils.randomAlphanumeric(64).getBytes(),
                        RandomStringUtils.randomAlphanumeric(64).getBytes());
        }
        throw new IllegalArgumentException("Unknown prop data type " + this);
    }
}
