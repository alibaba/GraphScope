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
package com.alibaba.maxgraph.compiler.prepare.store;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.CompilerConfig;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class PrepareEntity<V> implements Serializable {
    private static final long serialVersionUID = -2362003390845698421L;
    public static final int CONSTANT_INDEX = -1;

    /**
     * Index for argument in Value/LogicalCompare in proto
     */
    private final int argumentIndex;

    public PrepareEntity(int argumentIndex) {
        checkArgument(argumentIndex > 0, "argument index must > 0 for current value=>" + argumentIndex);
        this.argumentIndex = argumentIndex;
    }

    public int getArgumentIndex() {
        return argumentIndex;
    }

    public abstract List<Integer> getParamIndexList();

    public abstract V prepareParam(List<List<Object>> paramList, GraphSchema schema, CompilerConfig compilerConfig);
}
