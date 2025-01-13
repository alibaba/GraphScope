package com.alibaba.graphscope.groot.service.deserializer;

import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.PrimitiveType;
import com.alibaba.graphscope.groot.service.models.StringTypeString;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;



public class GSDataTypeDeserializer extends JsonDeserializer<GSDataType> {
    public GSDataTypeDeserializer() {
    }

    @Override
    public GSDataType deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();
        System.out.println("GSDataTypeDeserializer");
        System.out.println(node);

        if (node.has("string")) {
            StringType stringType = new StringType();
            stringType.setString(mapper.treeToValue(node.get("string"), StringTypeString.class));
            return stringType;
        } else if (node.has("temporal")) {
            return new TemporalType(jp.getCodec().treeToValue(node.get("temporal"), TemporalTypeTemporal.class));
        } else if (node.has("primitive_type")){
            return new PrimitiveType(jp.getCodec().treeToValue(node.get("primitive_type"), String.class));
        }
        else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}