package com.alibaba.graphscope.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class JsonUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        SimpleModule m = new SimpleModule();
        OBJECT_MAPPER.registerModule(m);
        OBJECT_MAPPER.registerModule(new ParanamerModule());
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JsonProcessingException : ", e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<T> typeRef) throws RuntimeException {
        try {
            return isEmpty(json) ? null : OBJECT_MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> valueType) {
        try {
            return OBJECT_MAPPER.readValue(json, valueType);
        } catch (IOException e) {
            throw new RuntimeException("IOException : ", e);
        }
    }

    public static JsonNode fromJson(String json) {
        try {
            return OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
            throw new RuntimeException("IOException : ", e);
        }
    }

    public static JsonNode parseJsonTree(String json) throws Exception {
        return isEmpty(json) ? null : OBJECT_MAPPER.readTree(json);
    }

    public static <T> List<T> parseAsList(String json, Class<T> valueType) throws IOException {
        JavaType t = OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, valueType);
        return OBJECT_MAPPER.readValue(json, t);
    }
}

