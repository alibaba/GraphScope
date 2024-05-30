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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicReference;

// When IrMeta is updated, Glogue will also be triggered to update. This class defines the specific
// operations for updating Glogue.
public class GlogueHolder implements IrMetaTracker {
    private final PlannerConfig config;
    private final AtomicReference<GlogueQuery> glogueRef;

    public GlogueHolder(Configs configs) {
        this.config = new PlannerConfig(configs);
        this.glogueRef = new AtomicReference<>();
    }

    @Override
    public void onChanged(IrMeta meta) {
        if (meta instanceof IrMetaStats) {
            GlogueSchema g = GlogueSchema.fromMeta((IrMetaStats) meta);
            Glogue gl = new Glogue(g, config.getGlogueSize());
            GlogueQuery gq = new GlogueQuery(gl);
            this.glogueRef.compareAndSet(glogueRef.get(), gq);
        }
    }

    public @Nullable GlogueQuery getGlogue() {
        return this.glogueRef.get();
    }
}
