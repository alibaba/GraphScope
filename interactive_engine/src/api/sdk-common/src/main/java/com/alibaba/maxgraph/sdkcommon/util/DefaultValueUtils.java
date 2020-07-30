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
package com.alibaba.maxgraph.sdkcommon.util;

import com.alibaba.maxgraph.sdkcommon.MaxGraphFunctional;
import com.alibaba.maxgraph.sdkcommon.exception.MaxGraphException;
import com.alibaba.maxgraph.sdkcommon.exception.MetaException;
import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils.ErrorCode.DefaultValueNotMatchDataType;

public class DefaultValueUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultValueUtils.class);

    public final static TypeReference<List<Integer>> LIST_INT = new TypeReference<List<Integer>>() {
    };

    public final static TypeReference<List<Long>> LIST_LONG = new TypeReference<List<Long>>() {
    };

    public final static TypeReference<List<Float>> LIST_FLOAT = new TypeReference<List<Float>>() {
    };

    public final static TypeReference<List<Double>> LIST_DOUBLE = new TypeReference<List<Double>>() {
    };

    public final static TypeReference<List<String>> LIST_STRING = new TypeReference<List<String>>() {
    };

    public final static TypeReference<List<Byte[]>> LIST_BYTES = new TypeReference<List<Byte[]>>() {
    };

    public final static String EXCEPTION_MESSAGE_FORMAT = "default value should be %s, for example: %s";

    public final static String EXCEPTION_MESSAGE_FORMAT_NO_DEFAULT_VALUE = "Currently not support default value for " +
            "type %s";

    public static Object checkDefaultValue(DataType dataType, String defaultValue) throws MaxGraphException {
        if (defaultValue != null) {
            switch (dataType.getType()) {
                case CHAR:
                    return checkList(dataType, defaultValue, () -> {
                        if (defaultValue.length() == 1) {
                            return defaultValue.charAt(0);
                        } else {
                            throw new RuntimeException("char can not use " + defaultValue + " as default value");
                        }
                    });
                case INT:
                    return checkList(dataType, defaultValue, () -> Integer.parseInt(defaultValue));
                case LONG:
                    return checkList(dataType, defaultValue, () -> Long.parseLong(defaultValue));
                case FLOAT:
                    return checkList(dataType, defaultValue, () -> Float.parseFloat(defaultValue));
                case SHORT:
                    return checkList(dataType, defaultValue, () -> Short.parseShort(defaultValue));
                case DOUBLE:
                    return checkList(dataType, defaultValue, () -> Double.parseDouble(defaultValue));
                case STRING:
                    return defaultValue;
                case LIST:
                    if (InternalDataType.INT.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_INT));
                    } else if (InternalDataType.LONG.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_LONG));
                    } else if (InternalDataType.FLOAT.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_FLOAT));
                    } else if (InternalDataType.DOUBLE.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_DOUBLE));
                    } else if (InternalDataType.STRING.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_STRING));
                    } else if (InternalDataType.BYTES.name().equals(dataType.getExpression())) {
                        return checkList(dataType, defaultValue, () -> JSON.fromJson(defaultValue, LIST_BYTES));
                    } else {
                        throw MetaException.dataTypeNotValid(dataType);
                    }
                case BOOL:
                    return checkList(dataType, defaultValue, () -> Boolean.parseBoolean(defaultValue));
                default:
                    throw new MaxGraphException(DefaultValueNotMatchDataType, getExceptionMessage(dataType));
            }
        }

        return null;
    }

    public static Object checkList(DataType dataType, String defaultValue, MaxGraphFunctional.Callable callable) throws MaxGraphException {
        try {
            if (defaultValue.length() == 0) {
                return null;
            }

            return callable.call();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new MaxGraphException(DefaultValueNotMatchDataType, getExceptionMessage(dataType));
        }
    }

    public static String getExceptionMessage(DataType dataType) {
        switch (dataType.getType()) {
            case SHORT:
            case INT:
            case LONG:
                return String.format(EXCEPTION_MESSAGE_FORMAT, JSON.toJson(dataType), 1);
            case DOUBLE:
            case FLOAT:
                return String.format(EXCEPTION_MESSAGE_FORMAT, JSON.toJson(dataType), 1.0);
            case BOOL:
                return String.format(EXCEPTION_MESSAGE_FORMAT, JSON.toJson(dataType), "true");
            case LIST:
                return String.format(EXCEPTION_MESSAGE_FORMAT, JSON.toJson(dataType), "LIST<INT> has Value [1, 2, 3, " +
                        "4]");
            case CHAR:
                return String.format(EXCEPTION_MESSAGE_FORMAT, JSON.toJson(dataType), "'c'");
            default:
                return String.format(EXCEPTION_MESSAGE_FORMAT_NO_DEFAULT_VALUE, JSON.toJson(dataType));
        }
    }
}
