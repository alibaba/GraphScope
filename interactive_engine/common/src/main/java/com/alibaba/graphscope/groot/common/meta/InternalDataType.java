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
package com.alibaba.graphscope.groot.common.meta;

import java.util.ArrayList;
import java.util.List;

public enum InternalDataType {

    /**
     * BOOL data type in InteractiveEngine, map to boolean in Java;
     */
    BOOL,
    /**
     * CHAR data type in InteractiveEngine, map to char(byte) in Java;
     */
    CHAR,
    /**
     * SHORT data type in InteractiveEngine, map to short in Java;
     */
    SHORT,
    /**
     * INT data type in InteractiveEngine, map to Integer(int) in Java
     */
    INT,
    /**
     * LONG data type in InteractiveEngine, map to Long(long) in Java
     */
    LONG,
    /**
     * FLOAT data type in InteractiveEngine, map to Float(float) in Java
     */
    FLOAT,
    /**
     * DOUBLE data type in InteractiveEngine, map to Double(double) in Java
     */
    DOUBLE,
    /**
     * BYTES data type in InteractiveEngine, map to byte[] in Java
     */
    BYTES,
    /**
     * STRING data type in InteractiveEngine, map to String in Java
     */
    STRING,

    /**
     * Date data type in InteractiveEngine, map to DateValue in Java
     */
    DATE,
    /**
     * Date data type in InteractiveEngine, map to TimeValue in Java
     */
    TIME,
    /**
     * Date data type in InteractiveEngine, map to DateTimeValue in Java
     */
    TIMESTAMP,

    /**
     * SET data type, Collection Type, can mixed with list and map, example:Set， value: List<Map<List<String>,List<String>>>
     */
    SET,

    /**
     *
     * LIST data type, Collection Type, can mixed with set and map
     */
    LIST,

    /**
     * MAP data type, Collection Type, can mixed with set and list
     */
    MAP,

    /**
     * Some data in special format; for example some well defined protobuf structure;
     */
    UNKNOWN;

    public static final List<String> primitiveTypes = new ArrayList<>();

    static {
        InternalDataType[] values = InternalDataType.values();

        for (InternalDataType value : values) {
            if (value != LIST
                    && value != SET
                    && value != MAP
                    && value != UNKNOWN
                    && value != CHAR
                    && value != DATE
                    && value != TIME
                    && value != TIMESTAMP
                    && value != SHORT) {
                primitiveTypes.add(value.name());
            }
        }
    }
}
