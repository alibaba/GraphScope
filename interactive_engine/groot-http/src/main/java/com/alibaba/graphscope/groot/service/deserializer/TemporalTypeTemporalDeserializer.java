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

import com.alibaba.graphscope.groot.service.models.DateType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.alibaba.graphscope.groot.service.models.TimeStampType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class TemporalTypeTemporalDeserializer extends JsonDeserializer<TemporalTypeTemporal> {
    public TemporalTypeTemporalDeserializer() {}

    @Override
    public TemporalTypeTemporal deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);

        if (node.has("date32")) {
            return new DateType();
        } else if (node.has("timestamp")) {
            return new TimeStampType();
        } else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}