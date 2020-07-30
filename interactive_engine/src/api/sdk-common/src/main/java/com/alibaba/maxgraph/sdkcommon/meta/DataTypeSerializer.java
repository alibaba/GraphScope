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
package com.alibaba.maxgraph.sdkcommon.meta;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class DataTypeSerializer extends StdSerializer<DataType> {

    public DataTypeSerializer() {
        super(DataType.class, true);
    }

    @Override
    public void serialize(DataType value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        if (value.isPrimitiveType()) {
            gen.writeString(value.name());
        } else {
            String prefix = "";
            if (value.getType().equals(InternalDataType.LIST)) {
                prefix = "LIST";
            } else if (value.getType().equals(InternalDataType.SET)) {
                prefix = "S";
            } else if (value.getType().equals(InternalDataType.MAP)) {
                prefix = "M";
            } else {
                throw new IOException(String.format("unknown data type: %s", value.toString()));
            }
            gen.writeString(String.format("%s<%s>", prefix, value.getExpression()));
        }
    }
}
