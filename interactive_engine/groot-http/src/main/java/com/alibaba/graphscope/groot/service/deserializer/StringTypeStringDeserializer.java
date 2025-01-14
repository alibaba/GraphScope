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

import com.alibaba.graphscope.groot.service.models.FixedChar;
import com.alibaba.graphscope.groot.service.models.FixedCharChar;
import com.alibaba.graphscope.groot.service.models.LongText;
import com.alibaba.graphscope.groot.service.models.StringTypeString;
import com.alibaba.graphscope.groot.service.models.VarChar;
import com.alibaba.graphscope.groot.service.models.VarCharVarChar;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class StringTypeStringDeserializer extends JsonDeserializer<StringTypeString> {
    public StringTypeStringDeserializer() {}

    @Override
    public StringTypeString deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);

        if (node.has("var_char")) {
            System.out.println("var_char");
            VarCharVarChar varChar = new VarCharVarChar();
            if (node.get("var_char").has("max_length")) {
                varChar.setMaxLength(node.get("var_char").get("max_length").asInt());
            } else {
                throw new IOException("max_length not found in var_char");
            }
            return new VarChar(varChar);
        } else if (node.has("long_text")) {
            return new LongText(jp.getCodec().treeToValue(node.get("long_text"), String.class));
        } else if (node.has("fixed_char")) {
            return new FixedChar(
                    jp.getCodec().treeToValue(node.get("fixed_char"), FixedCharChar.class));
        } else {
            throw new IOException("Unknown variant for GSDataType");
        }
    }
}
