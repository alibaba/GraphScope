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
package com.alibaba.graphscope.gaia.idmaker;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.concurrent.atomic.AtomicLong;

public class IncrementalQueryIdMaker implements IdMaker<Long, Traversal.Admin> {
    private static final AtomicLong idCounter = new AtomicLong(0L);

    @Override
    public Long getId(Traversal.Admin traversal) {
        return idCounter.incrementAndGet();
    }
}
