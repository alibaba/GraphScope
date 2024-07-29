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

package com.alibaba.graphscope.common.ir.meta.fetcher;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;

import java.util.Optional;

/**
 * This interface primarily describes the strategies to obtaining IrMeta, which are mainly of two types: static and dynamic.
 * The static strategy {@link StaticIrMetaFetcher} assumes that IrMeta does not change after initialization,
 * while the dynamic strategy {@link DynamicIrMetaFetcher} assumes that IrMeta can change.
 */
public abstract class IrMetaFetcher {
    protected final IrMetaReader reader;
    protected final IrMetaTracker tracker;

    protected IrMetaFetcher(IrMetaReader reader, IrMetaTracker tracker) {
        this.reader = reader;
        this.tracker = tracker;
    }

    public abstract Optional<IrMeta> fetch();
}
