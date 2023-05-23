/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.schema.procedure;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class StoredProcedureMeta {
    private final String name;
    private final RelDataType returnType;
    private final List<Parameter> parameters;

    public StoredProcedureMeta(String name, RelDataType returnType, List<Parameter> parameters) {
        this.name = name;
        this.returnType = returnType;
        this.parameters = Objects.requireNonNull(parameters);
    }

    public static class Parameter {
        private final String name;
        private final RelDataType dataType;

        public Parameter(String name, RelDataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }

        public String getName() {
            return name;
        }

        public RelDataType getDataType() {
            return dataType;
        }
    }

    public String getName() {
        return name;
    }

    public RelDataType getReturnType() {
        return returnType;
    }

    public List<Parameter> getParameters() {
        return Collections.unmodifiableList(parameters);
    }
}
