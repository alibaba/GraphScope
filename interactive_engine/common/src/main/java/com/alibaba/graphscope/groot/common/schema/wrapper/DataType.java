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
package com.alibaba.graphscope.groot.common.schema.wrapper;

import com.alibaba.graphscope.groot.common.exception.InvalidDataTypeException;
import com.alibaba.graphscope.groot.common.exception.UnsupportedOperationException;
import com.alibaba.graphscope.groot.common.meta.InternalDataType;
import com.alibaba.graphscope.proto.groot.DataTypePb;

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
    DATE(16), // represent DATE32
    TIME32(17),
    TIMESTAMP(18),
    UINT(19),
    ULONG(20);

    private final byte type;

    DataType(int type) {
        this.type = (byte) type;
    }

    public static DataType parseProto(DataTypePb pb) {
        switch (pb) {
            case UNKNOWN:
                return DataType.UNKNOWN;
            case BOOL:
                return DataType.BOOL;
            case CHAR:
                return DataType.CHAR;
            case SHORT:
                return DataType.SHORT;
            case INT:
                return DataType.INT;
            case LONG:
                return DataType.LONG;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
                return DataType.DOUBLE;
            case STRING:
                return DataType.STRING;
            case BYTES:
                return DataType.BYTES;
            case INT_LIST:
                return DataType.INT_LIST;
            case LONG_LIST:
                return DataType.LONG_LIST;
            case FLOAT_LIST:
                return DataType.FLOAT_LIST;
            case DOUBLE_LIST:
                return DataType.DOUBLE_LIST;
            case STRING_LIST:
                return DataType.STRING_LIST;
            case DATE32:
                return DataType.DATE;
            case TIME32_MS:
                return DataType.TIME32;
            case TIMESTAMP_MS:
                return DataType.TIMESTAMP;
            case UINT:
                return DataType.UINT;
            case ULONG:
                return DataType.ULONG;
            default:
                throw new InvalidDataTypeException("Unknown DataType: [" + pb + "]");
        }
    }

    public static DataType parseString(String type) {
        String upperType = type.toUpperCase();
        if (upperType.startsWith("LIST<")) {
            // "LIST<value>" -> value
            String typeValue = upperType.substring(5, upperType.length() - 1) + "_LIST";
            return DataType.valueOf(typeValue);
        } else {
            return DataType.valueOf(type);
        }
    }

    public DataTypePb toProto() {
        switch (this) {
            case UNKNOWN:
                return DataTypePb.UNKNOWN;
            case BOOL:
                return DataTypePb.BOOL;
            case CHAR:
                return DataTypePb.CHAR;
            case SHORT:
                return DataTypePb.SHORT;
            case INT:
                return DataTypePb.INT;
            case LONG:
                return DataTypePb.LONG;
            case FLOAT:
                return DataTypePb.FLOAT;
            case DOUBLE:
                return DataTypePb.DOUBLE;
            case STRING:
                return DataTypePb.STRING;
            case BYTES:
                return DataTypePb.BYTES;
            case INT_LIST:
                return DataTypePb.INT_LIST;
            case LONG_LIST:
                return DataTypePb.LONG_LIST;
            case FLOAT_LIST:
                return DataTypePb.FLOAT_LIST;
            case DOUBLE_LIST:
                return DataTypePb.DOUBLE_LIST;
            case STRING_LIST:
                return DataTypePb.STRING_LIST;
            case DATE:
                return DataTypePb.DATE32;
            case TIME32:
                return DataTypePb.TIME32_MS;
            case TIMESTAMP:
                return DataTypePb.TIMESTAMP_MS;
            case UINT:
                return DataTypePb.UINT;
            case ULONG:
                return DataTypePb.ULONG;
            default:
                throw new UnsupportedOperationException("Unsupported DataType: [" + this + "]");
        }
    }

    @Override
    public String toString() {
        String dataTypeString = this.name();
        if (dataTypeString.endsWith("_LIST")) {
            String[] dataTypeArray = dataTypeString.split("_");
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
            case SHORT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case LONG:
            case DOUBLE:
                return 8;
            default:
                throw new UnsupportedOperationException("not a fixed length type [" + this + "]");
        }
    }

    public static DataType parseFromDataType(
            com.alibaba.graphscope.groot.common.meta.DataType dataType) {
        InternalDataType internalDataType = dataType.getType();
        switch (internalDataType) {
            case BOOL:
                return DataType.BOOL;

            case CHAR:
                return DataType.CHAR;

            case SHORT:
                return DataType.SHORT;

            case INT:
                return DataType.INT;

            case LONG:
                return DataType.LONG;

            case FLOAT:
                return DataType.FLOAT;

            case DOUBLE:
                return DataType.DOUBLE;

            case STRING:
                return DataType.STRING;

            case DATE:
                return DataType.DATE;

            case TIME:
                return DataType.TIME32;

            case TIMESTAMP:
                return DataType.TIMESTAMP;

            case BYTES:
                return DataType.BYTES;

            case LIST:
                {
                    switch (InternalDataType.valueOf(dataType.getExpression())) {
                        case INT:
                            {
                                return DataType.INT_LIST;
                            }
                        case LONG:
                            {
                                return DataType.LONG_LIST;
                            }
                        case FLOAT:
                            {
                                return DataType.FLOAT_LIST;
                            }
                        case DOUBLE:
                            {
                                return DataType.DOUBLE_LIST;
                            }
                        case STRING:
                            {
                                return DataType.STRING_LIST;
                            }
                        default:
                            {
                                throw new InvalidDataTypeException(
                                        "Unsupported property data type " + dataType);
                            }
                    }
                }
            default:
                {
                    throw new InvalidDataTypeException(
                            "Unsupported property data type " + dataType);
                }
        }
    }
}
