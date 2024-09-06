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
import com.google.common.collect.Lists;

import java.util.List;

public class SchemaSpecManager {
    private final IrGraphSchema parent;
    private final List<SchemaSpec> specifications;

    public SchemaSpecManager(IrGraphSchema parent, SchemaSpec input) {
        this.parent = parent;
        this.specifications = Lists.newArrayList(input);
    }

    public SchemaSpec getSpec(SchemaSpec.Type type) {
        // if not exist, try to register it
        for (SchemaSpec spec : specifications) {
            if (spec.getType() == type) {
                return spec;
            }
        }
        // create a new JsonSpecification with content converted from others

    }

    private SchemaSpec convert(SchemaSpec source, SchemaSpec.Type target) {
        if (source.getType() == target) {
            return source;
        }
        switch (target) {
            case IR_CORE_IN_JSON:
                return new SchemaSpec(
                        target,
                        IrSchemaParser.getInstance()
                                .parse(parent.getGraphSchema(), parent.isColumnId()));
            case FLEX_IN_JSON:
                if (source.getType() == SchemaSpec.Type.FLEX_IN_YAML) {}

                throw new UnsupportedOperationException(
                        "cannot convert schema specification from ["
                                + source.getType()
                                + "]"
                                + " to ["
                                + target
                                + "]");
            case FLEX_IN_YAML:
                if (source.getType() == SchemaSpec.Type.FLEX_IN_YAML) {}

                throw new UnsupportedOperationException(
                        "cannot convert schema specification from ["
                                + source.getType()
                                + "]"
                                + " to ["
                                + target
                                + "]");
        }
    }
}
