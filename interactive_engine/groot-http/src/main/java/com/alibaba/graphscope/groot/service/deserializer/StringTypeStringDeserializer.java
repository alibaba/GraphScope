package com.alibaba.graphscope.groot.service.deserializer;

import com.alibaba.graphscope.groot.service.models.GSDataType;
import com.alibaba.graphscope.groot.service.models.StringTypeString;
import com.alibaba.graphscope.groot.service.models.VarChar;
import com.alibaba.graphscope.groot.service.models.VarCharVarChar;
import com.alibaba.graphscope.groot.service.models.FixedCharChar;
import com.alibaba.graphscope.groot.service.models.LongText;
import com.alibaba.graphscope.groot.service.models.FixedChar;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;



public class StringTypeStringDeserializer extends JsonDeserializer<StringTypeString> {
    public StringTypeStringDeserializer() {
    }

    @Override
    public StringTypeString deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        System.out.println("StringTypeStringDeserializer");
        System.out.println(node);

        if (node.has("var_char")) {
            System.out.println("var_char");
            VarCharVarChar varChar = new VarCharVarChar();
            if (node.get("var_char").has("max_length")) {
                // return new VarChar(new VarCharVarChar(jp.getCodec().treeToValue(node.get("var_char").get("max_length"), String.class)));
                varChar.setMaxLength(node.get("var_char").get("max_length").asInt());
            }
            else {
                throw new IOException("max_length not found in var_char");
            }
            return new VarChar(varChar);
        } else if (node.has("long_text")) {
            return new LongText(jp.getCodec().treeToValue(node.get("long_text"), String.class));
        } else if (node.has("fixed_char")){
            return new FixedChar(jp.getCodec().treeToValue(node.get("fixed_char"), FixedCharChar.class));
        } else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}