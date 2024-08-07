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
package com.alibaba.graphscope.groot.common.util;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;

import java.io.IOException;
import java.util.List;

public class JSON {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        SimpleModule m = new SimpleModule();
        OBJECT_MAPPER.registerModule(m);
        OBJECT_MAPPER.registerModule(new ParanamerModule());
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new InvalidArgumentException("JsonProcessingException : ", e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<T> typeRef) throws RuntimeException {
        try {
            return (json == null || json.isEmpty()) ? null : OBJECT_MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            throw new InvalidArgumentException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> valueType) {
        try {
            return OBJECT_MAPPER.readValue(json, valueType);
        } catch (IOException e) {
            throw new InvalidArgumentException("IOException : ", e);
        }
    }

    public static JsonNode fromJson(String json) {
        try {
            return OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
            throw new InvalidArgumentException("IOException : ", e);
        }
    }

    public static JsonNode parseJsonTree(String json) throws Exception {
        return (json == null || json.isEmpty()) ? null : OBJECT_MAPPER.readTree(json);
    }

    public static <T> List<T> parseAsList(String json, Class<T> valueType) throws IOException {
        JavaType t = OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, valueType);
        return OBJECT_MAPPER.readValue(json, t);
    }
}
