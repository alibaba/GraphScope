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

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Periodically update IrMeta, with the update frequency controlled by configuration.
 * Specifically, for procedures, a remote update will be actively triggered when they are not found locally.
 */
public class DynamicIrMetaFetcher extends IrMetaFetcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DynamicIrMetaFetcher.class);
    private final ScheduledExecutorService scheduler;
    private volatile IrMetaStats currentState;
    // To manage the state changes of statistics resulting from different update operations.
    private volatile StatsState statsState;

    public DynamicIrMetaFetcher(Configs configs, IrMetaReader dataReader, IrMetaTracker tracker) {
        super(dataReader, tracker);
        this.scheduler = new ScheduledThreadPoolExecutor(2);
        this.scheduler.scheduleAtFixedRate(
                () -> syncMeta(),
                0,
                GraphConfig.GRAPH_META_SCHEMA_FETCH_INTERVAL_MS.get(configs),
                TimeUnit.MILLISECONDS);
        this.scheduler.scheduleAtFixedRate(
                () -> syncStats(),
                0,
                GraphConfig.GRAPH_META_STATISTICS_FETCH_INTERVAL_MS.get(configs),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<IrMeta> fetch() {
        return currentState == null ? Optional.empty() : Optional.of(currentState);
    }

    private synchronized void syncMeta() {
        try {
            IrMeta meta = this.reader.readMeta();
            GraphStatistics curStats;
            // if the graph id is changed, we need to update the statistics
            if (this.currentState == null
                    || !this.currentState.getGraphId().equals(meta.getGraphId())) {
                this.statsState = StatsState.INITIALIZED;
                curStats = null;
            } else {
                curStats = this.currentState.getStatistics();
            }
            this.currentState =
                    new IrMetaStats(
                            meta.getGraphId(),
                            meta.getSnapshotId(),
                            meta.getSchema(),
                            meta.getStoredProcedures(),
                            curStats);
            if (this.statsState != StatsState.SYNCED) {
                syncStats();
            }
        } catch (Exception e) {
            logger.warn("failed to read meta data, error is {}", e);
        }
    }

    private synchronized void syncStats() {
        try {
            if (this.currentState != null) {
                GraphStatistics stats = this.reader.readStats(this.currentState.getGraphId());
                if (stats != null) {
                    this.currentState =
                            new IrMetaStats(
                                    this.currentState.getSnapshotId(),
                                    this.currentState.getSchema(),
                                    this.currentState.getStoredProcedures(),
                                    stats);
                    if (tracker != null) {
                        tracker.onChanged(this.currentState);
                    }
                    this.statsState = StatsState.SYNCED;
                }
            }
        } catch (Exception e) {
            logger.warn("failed to read graph statistics, error is {}", e);
        } finally {
            if (this.currentState != null
                    && tracker != null
                    && this.statsState == StatsState.INITIALIZED) {
                tracker.onChanged(this.currentState);
                this.statsState = StatsState.MOCKED;
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.scheduler.shutdown();
    }

    public enum StatsState {
        INITIALIZED, // first initialized or graph id changed
        MOCKED, // the switch can only occur from the INITIALIZED state. If remote statistics are
        // unavailable, a mocked statistics object is created once.
        SYNCED // remote stats is synced
    }
}
