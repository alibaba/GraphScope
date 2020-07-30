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

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.compiler.optimizer.CompilerConfig;
import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PrepareLongListEntity extends PrepareEntity<Message.Value> {
    private int paramIndex;

    public PrepareLongListEntity(int argumentIndex, int paramIndex) {
        super(argumentIndex);

        checkArgument(paramIndex > 0, "param index must > 0 for current param index=>" + paramIndex);
        this.paramIndex = paramIndex;
    }

    @Override
    public List<Integer> getParamIndexList() {
        return Lists.newArrayList(this.paramIndex);
    }

    @Override
    public Message.Value prepareParam(List<List<Object>> paramList, GraphSchema schema, CompilerConfig compilerConfig) {
        checkArgument(paramIndex <= paramList.size(), "Can't get param[" + paramIndex + "] from " + paramList);
        Message.Value.Builder builder = Message.Value.newBuilder().setIndex(getArgumentIndex());
        for (Object obj : paramList.get(paramIndex - 1)) {
            builder.addLongValueList(Long.parseLong(obj.toString()));
        }
        return builder.build();
    }
}
