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
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class StaticIrMetaFetcher extends IrMetaFetcher {
    private static final Logger logger = LoggerFactory.getLogger(StaticIrMetaFetcher.class);
    private final IrMetaStats metaStats;

    public StaticIrMetaFetcher(IrMetaReader dataReader, IrMetaTracker tracker) throws IOException {
        super(dataReader, tracker);
        IrMeta meta = this.reader.readMeta();
        GraphStatistics stats = (meta == null) ? null : fetchStats(meta);
        this.metaStats =
                new IrMetaStats(
                        meta.getSnapshotId(), meta.getSchema(), meta.getStoredProcedures(), stats);
        if (tracker != null && stats != null) {
            tracker.onChanged(this.metaStats);
        }
    }

    private @Nullable GraphStatistics fetchStats(IrMeta meta) {
        try {
            return this.reader.readStats(meta.getGraphId());
        } catch (Exception e) {
            logger.warn("failed to read graph statistics, error is {}", e);
            return null;
        }
    }

    @Override
    public Optional<IrMeta> fetch() {
        return Optional.of(this.metaStats);
    }
}
