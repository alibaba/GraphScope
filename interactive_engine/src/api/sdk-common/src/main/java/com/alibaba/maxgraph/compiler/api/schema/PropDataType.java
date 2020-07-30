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

import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Date;

public enum PropDataType {
    BOOL,
    CHAR,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BINARY,
    STRING,
    DATE,
    INTEGER_LIST,
    LONG_LIST,
    FLOAT_LIST,
    DOUBLE_LIST,
    STRING_LIST,
    BYTES_LIST;

    public static PropDataType parseFromDataType(DataType dataType) {
        InternalDataType internalDataType = dataType.getType();
        switch (internalDataType) {
            case BOOL: {
                return PropDataType.BOOL;
            }
            case CHAR: {
                return PropDataType.CHAR;
            }
            case SHORT: {
                return PropDataType.SHORT;
            }
            case INT: {
                return PropDataType.INTEGER;
            }
            case LONG: {
                return PropDataType.LONG;
            }
            case FLOAT: {
                return PropDataType.FLOAT;
            }
            case DOUBLE: {
                return PropDataType.DOUBLE;
            }
            case STRING: {
                return PropDataType.STRING;
            }
            case BYTES: {
                return PropDataType.BINARY;
            }
            case DATE: {
                return PropDataType.STRING;
            }
            case LIST: {
                switch (InternalDataType.valueOf(dataType.getExpression())) {
                    case INT: {
                        return PropDataType.INTEGER_LIST;
                    }
                    case LONG: {
                        return PropDataType.LONG_LIST;
                    }
                    case FLOAT: {
                        return PropDataType.FLOAT_LIST;
                    }
                    case DOUBLE: {
                        return PropDataType.DOUBLE_LIST;
                    }
                    case STRING: {
                        return PropDataType.STRING_LIST;
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
            case INTEGER:
                return RandomUtils.nextInt();
            case LONG:
                return RandomUtils.nextLong();
            case FLOAT:
                return RandomUtils.nextFloat();
            case DOUBLE:
                return RandomUtils.nextDouble();
            case STRING:
                return RandomStringUtils.randomAlphanumeric(64);
            case BINARY:
                return RandomStringUtils.random(64).getBytes();
            case INTEGER_LIST:
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
