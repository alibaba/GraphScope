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

import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.common.exception.InvalidDataException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class DataTypeDeserializer extends StdDeserializer<DataType> {
    public DataTypeDeserializer() {
        this(null);
    }

    public DataTypeDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public DataType deserialize(JsonParser p, DeserializationContext ctxt) throws GrootException {
        // todo: only support List<T> and Set<T> currently.
        try {
            String data = p.getValueAsString();
            if (data.startsWith("LIST<")) {
                DataType ret = new DataType(InternalDataType.LIST);
                ret.setExpression(data.substring("LIST<".length(), data.length() - 1));
                return ret;
            } else if (data.startsWith("S<")) {
                DataType ret = new DataType(InternalDataType.SET);
                ret.setExpression(data.substring(2, data.length() - 1));
                return ret;
            } else if (data.startsWith("M<")) {

            } else {
                DataType ret = new DataType(InternalDataType.valueOf(data.toUpperCase()));
                return ret;
            }
        } catch (Exception ex) {
            throw new InvalidDataException(ex);
        }
        return null;
    }
}
