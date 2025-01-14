/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.groot.service.deserializer;

import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.PrimitiveType;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.StringTypeString;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class GSDataTypeDeserializer extends JsonDeserializer<GSDataType> {
    public GSDataTypeDeserializer() {}

    @Override
    public GSDataType deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();

        if (node.has("string")) {
            StringType stringType = new StringType();
            stringType.setString(mapper.treeToValue(node.get("string"), StringTypeString.class));
            return stringType;
        } else if (node.has("temporal")) {
            // ObjectMapper objectMapper = new ObjectMapper();
            TemporalType temporalType = new TemporalType();
            temporalType.setTemporal(
                    mapper.treeToValue(node.get("temporal"), TemporalTypeTemporal.class));
            return temporalType;
        } else if (node.has("primitive_type")) {
            return new PrimitiveType(
                    PrimitiveType.PrimitiveTypeEnum.fromValue(
                            jp.getCodec().treeToValue(node.get("primitive_type"), String.class)));
        } else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}
