/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.groot.common.schema.api.GraphSchema;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.yaml.snakeyaml.Yaml;

import java.util.Map;

public class SchemaSpec {
    private final Type type;
    private final String content;

    public SchemaSpec(Type type, String content) {
        this.type = type;
        this.content = content;
    }

    public GraphSchema convert() throws JacksonException {
        switch (type) {
            case IR_CORE_IN_JSON:
                return Utils.buildSchemaFromJson(content);
            case FLEX_IN_YAML:
                return Utils.buildSchemaFromYaml(content);
            case FLEX_IN_JSON:
            default:
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(content);
                Map rootMap = mapper.convertValue(rootNode, Map.class);
                Yaml yaml = new Yaml();
                return Utils.buildSchemaFromYaml(yaml.dump(rootMap));
        }
    }

    public String getContent() {
        return content;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        IR_CORE_IN_JSON,
        FLEX_IN_JSON,
        FLEX_IN_YAML;
    }
}
