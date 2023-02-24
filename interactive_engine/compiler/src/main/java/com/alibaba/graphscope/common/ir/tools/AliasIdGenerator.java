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

package com.alibaba.graphscope.common.ir.tools;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AliasIdGenerator {
    private final AtomicInteger idGenerator;

    public AliasIdGenerator() {
        this.idGenerator = new AtomicInteger();
    }

    public int generate(@Nullable String aliasName, @Nullable RelNode input) {
        if (aliasName == null || aliasName == AliasInference.DEFAULT_NAME) {
            return AliasInference.DEFAULT_ID;
        }
        List<RelNode> inputsQueue = new ArrayList<>();
        if (input != null) {
            inputsQueue.add(input);
        }
        while (!inputsQueue.isEmpty()) {
            RelNode cur = inputsQueue.remove(0);
            for (RelDataTypeField field : cur.getRowType().getFieldList()) {
                if (aliasName.equals(field.getName())) {
                    return field.getIndex();
                }
            }
            inputsQueue.addAll(cur.getInputs());
        }
        return idGenerator.getAndIncrement();
    }
}
