package com.alibaba.graphscope.groot.service.deserializer;

import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.alibaba.graphscope.groot.service.models.TimeStampType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.alibaba.graphscope.groot.service.models.DateType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;



public class TemporalTypeTemporalDeserializer extends JsonDeserializer<TemporalTypeTemporal> {
    public TemporalTypeTemporalDeserializer() {
    }

    @Override
    public TemporalTypeTemporal deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);

        if (node.has("date32")) {
            return jp.getCodec().treeToValue(node, DateType.class);
        } else if (node.has("timestamp")) {
            return jp.getCodec().treeToValue(node, TimeStampType.class);
        } else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}