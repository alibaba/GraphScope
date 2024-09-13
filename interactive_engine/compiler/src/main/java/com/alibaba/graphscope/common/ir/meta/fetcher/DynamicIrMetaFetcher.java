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
import com.alibaba.graphscope.common.ir.meta.GraphId;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaStats;
import com.alibaba.graphscope.common.ir.meta.IrMetaTracker;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.SchemaSpec.Type;
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
    private volatile Boolean statsEnabled = null;

    public DynamicIrMetaFetcher(Configs configs, IrMetaReader dataReader, IrMetaTracker tracker) {
        super(dataReader, tracker);
        this.scheduler = new ScheduledThreadPoolExecutor(2);
        this.scheduler.scheduleAtFixedRate(
                () -> syncMeta(),
                2000,
                GraphConfig.GRAPH_META_SCHEMA_FETCH_INTERVAL_MS.get(configs),
                TimeUnit.MILLISECONDS);
        this.scheduler.scheduleAtFixedRate(
                () -> syncStats(statsEnabled == null ? false : statsEnabled),
                2000,
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
            logger.debug(
                    "schema from remote: {}",
                    (meta == null) ? null : meta.getSchema().getSchemaSpec(Type.IR_CORE_IN_JSON));
            GraphStatistics curStats;
            // if the graph id or schema version is changed, we need to update the statistics
            if (this.currentState == null
                    || !this.currentState.getGraphId().equals(meta.getGraphId())
                    || !this.currentState
                            .getSchema()
                            .getVersion()
                            .equals(meta.getSchema().getVersion())) {
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
            boolean statsEnabled = getStatsEnabled(this.currentState.getGraphId());
            if (statsEnabled && this.statsState != StatsState.SYNCED
                    || (!statsEnabled && this.statsState != StatsState.MOCKED)) {
                logger.debug("start to sync stats");
                syncStats(statsEnabled);
            }
        } catch (Throwable e) {
            logger.warn("failed to read meta data, error is {}", e);
        }
    }

    private boolean getStatsEnabled(GraphId graphId) {
        try {
            return this.statsEnabled == null
                    ? this.reader.syncStatsEnabled(graphId)
                    : this.statsEnabled;
        } catch (
                Throwable e) { // if errors happen when reading stats enabled, we assume it is false
            logger.warn("failed to read stats enabled, error is {}", e);
            return false;
        }
    }

    private synchronized void syncStats(boolean statsEnabled) {
        try {
            if (this.currentState != null && statsEnabled) {
                GraphStatistics stats = this.reader.readStats(this.currentState.getGraphId());
                logger.debug("statistics from remote: {}", stats);
                if (stats != null && stats.getVertexCount() != 0) {
                    this.currentState =
                            new IrMetaStats(
                                    this.currentState.getSnapshotId(),
                                    this.currentState.getSchema(),
                                    this.currentState.getStoredProcedures(),
                                    stats);
                    if (tracker != null) {
                        logger.debug("start to update the glogue");
                        tracker.onChanged(this.currentState);
                    }
                    this.statsState = StatsState.SYNCED;
                }
            }
        } catch (Throwable e) {
            logger.warn("failed to read graph statistics, error is {}", e);
        } finally {
            try {
                if (this.currentState != null
                        && tracker != null
                        && this.statsState == StatsState.INITIALIZED) {
                    logger.debug("start to mock the glogue");
                    tracker.onChanged(this.currentState);
                    this.statsState = StatsState.MOCKED;
                }
            } catch (Throwable t) {
                logger.warn("failed to mock the glogue, error is {}", t);
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
