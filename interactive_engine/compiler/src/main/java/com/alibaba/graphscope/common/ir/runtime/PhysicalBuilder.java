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

package com.alibaba.graphscope.common.ir.runtime;

import com.alibaba.graphscope.common.ir.tools.LogicalPlan;

import org.apache.commons.lang3.StringUtils;

/**
 * build physical plan from logical plan
 * @param <R> is the actual type of physical plan
 */
public abstract class PhysicalBuilder<R> implements AutoCloseable {
    protected final LogicalPlan logicalPlan;

    protected PhysicalBuilder(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    /**
     * print physical plan
     */
    public abstract String explain();

    /**
     * build physical plan
     * @return
     */
    public abstract R build();

    public static final PhysicalBuilder createEmpty(LogicalPlan logicalPlan) {
        return new PhysicalBuilder(logicalPlan) {
            @Override
            public String explain() {
                return StringUtils.EMPTY;
            }

            @Override
            public Object build() {
                return null;
            }

            @Override
            public void close() throws Exception {}
        };
    }
}
