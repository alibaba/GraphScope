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

import com.alibaba.graphscope.groot.common.util.IrSchemaParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;

public class SchemaSpecManager {
    private static final Logger logger = LoggerFactory.getLogger(SchemaSpecManager.class);
    private final IrGraphSchema parent;
    private final List<SchemaSpec> specifications;

    public SchemaSpecManager(IrGraphSchema parent) {
        this.parent = parent;
        this.specifications = Lists.newArrayList(convert(null, SchemaSpec.Type.IR_CORE_IN_JSON));
    }

    public SchemaSpecManager(IrGraphSchema parent, SchemaSpec input) {
        this.parent = parent;
        this.specifications = Lists.newArrayList(input);
    }

    public SchemaSpec getSpec(SchemaSpec.Type type) {
        for (SchemaSpec spec : specifications) {
            if (spec.getType() == type) {
                return spec;
            }
        }
        // if not exist, try to create a new JsonSpecification with content converted from others
        SchemaSpec newSpec;
        List<SchemaSpec.Type> existing = Lists.newArrayList();
        for (SchemaSpec spec : specifications) {
            if ((newSpec = convert(spec, type)) != null) {
                specifications.add(newSpec);
                return newSpec;
            }
            existing.add(spec.getType());
        }
        throw new IllegalArgumentException(
                "spec type ["
                        + type
                        + "] cannot be converted from any existing spec types "
                        + existing);
    }

    private @Nullable SchemaSpec convert(@Nullable SchemaSpec source, SchemaSpec.Type target) {
        try {
            if (source != null && source.getType() == target) {
                return source;
            }
            switch (target) {
                case IR_CORE_IN_JSON:
                    return new SchemaSpec(
                            target,
                            IrSchemaParser.getInstance()
                                    .parse(parent.getGraphSchema(), parent.isColumnId()));
                case FLEX_IN_JSON:
                    if (source.getType() == SchemaSpec.Type.FLEX_IN_YAML) {
                        Yaml yaml = new Yaml();
                        Map rootMap = yaml.load(source.getContent());
                        ObjectMapper mapper = new ObjectMapper();
                        return new SchemaSpec(target, mapper.writeValueAsString(rootMap));
                    }
                    // todo: convert from JSON in IR_CORE to JSON in FLEX
                    return null;
                case FLEX_IN_YAML:
                default:
                    if (source.getType() == SchemaSpec.Type.FLEX_IN_JSON) {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode rootNode = mapper.readTree(source.getContent());
                        Map rootMap = mapper.convertValue(rootNode, Map.class);
                        Yaml yaml = new Yaml();
                        return new SchemaSpec(target, yaml.dump(rootMap));
                    }
                    // todo: convert from JSON in IR_CORE to YAML in FlEX
                    return null;
            }
        } catch (Exception e) {
            logger.warn(
                    "can not convert from {} to {} due to some unexpected exception:",
                    source.getType(),
                    target,
                    e);
            return null;
        }
    }
}
