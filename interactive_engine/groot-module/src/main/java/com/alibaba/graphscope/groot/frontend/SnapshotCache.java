/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.SnapshotListener;
import com.alibaba.graphscope.groot.SnapshotWithSchema;
import com.alibaba.graphscope.groot.common.exception.IllegalStateException;
import com.alibaba.graphscope.groot.common.schema.api.GraphStatistics;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/** A cache of querySnapshotId on Frontend node. */
public class SnapshotCache {

    public static final Logger logger = LoggerFactory.getLogger(SnapshotCache.class);

    private final AtomicReference<SnapshotWithSchema> snapshotWithSchemaRef;

    private final AtomicReference<GraphStatistics> graphStatisticsRef;

    private final TreeMap<Long, List<SnapshotListener>> snapshotToListeners;

    public SnapshotCache() {
        SnapshotWithSchema snapshotWithSchema = SnapshotWithSchema.newBuilder().build();
        snapshotWithSchemaRef = new AtomicReference<>(snapshotWithSchema);
        graphStatisticsRef = new AtomicReference<>();
        this.snapshotToListeners = new TreeMap<>();
    }

    public void addListener(long snapshotId, SnapshotListener listener) {
        synchronized (this.snapshotToListeners) {
            SnapshotWithSchema snapshotWithSchema = snapshotWithSchemaRef.get();
            long querySnapshotId = snapshotWithSchema.getSnapshotId();
            if (querySnapshotId >= snapshotId) {
                listener.onSnapshotAvailable();
                return;
            }
            List<SnapshotListener> listeners =
                    this.snapshotToListeners.computeIfAbsent(snapshotId, k -> new ArrayList<>());
            listeners.add(listener);
        }
    }

    /**
     * This method will update querySnapshotId and returns the previous value.
     *
     * <p>SnapshotManager will gather ingest progresses from all the WriterAgents, and calculate the
     * available querySnapshotId. Then SnapshotManager will call this method to update
     * querySnapshotId for each Frontend node.
     *
     * <p>Discussion:
     *
     * <p>We need to decide whether should the writing framework coupled with the implementation of
     * schema synchronization. Options are discussed here:
     * <a href="https://yuque.antfin-inc.com/graphscope/project/eibfty#EQGg9">Schema synchronization</a> This interface assumes write
     * framework isn't coupled with schema synchronization.
     *
     * @param snapshotId
     * @param graphDef
     * @return
     */
    public synchronized long advanceQuerySnapshotId(long snapshotId, GraphDef graphDef) {
        SnapshotWithSchema snapshotWithSchema = this.snapshotWithSchemaRef.get();
        long currentSnapshotId = snapshotWithSchema.getSnapshotId();
        if (currentSnapshotId >= snapshotId) {
            throw new IllegalStateException(
                    "current currentSnapshotId ["
                            + currentSnapshotId
                            + "], cannot update to ["
                            + snapshotId
                            + "]");
        }
        SnapshotWithSchema.Builder newSnapshotInfoBuilder =
                SnapshotWithSchema.newBuilder(snapshotWithSchema);
        newSnapshotInfoBuilder.setSnapshotId(snapshotId);
        GraphDef oldGraphDef = snapshotWithSchema.getGraphDef();
        if (graphDef != null
                && (oldGraphDef == null
                        || graphDef.getSchemaVersion()
                                > Long.parseLong(oldGraphDef.getVersion()))) {
            newSnapshotInfoBuilder.setGraphDef(graphDef);
            logger.info("schema updated. schema version [" + graphDef.getVersion() + "]");
            logger.debug(graphDef.toProto().toString());
        }
        this.snapshotWithSchemaRef.set(newSnapshotInfoBuilder.build());
        logger.debug("snapshotId update to [" + snapshotId + "]");
        synchronized (this.snapshotToListeners) {
            NavigableMap<Long, List<SnapshotListener>> listenersToTrigger =
                    this.snapshotToListeners.headMap(snapshotId, true);
            for (Map.Entry<Long, List<SnapshotListener>> listenerEntry :
                    listenersToTrigger.entrySet()) {
                List<SnapshotListener> listeners = listenerEntry.getValue();
                long listenSnapshotId = listenerEntry.getKey();
                for (SnapshotListener listener : listeners) {
                    try {
                        listener.onSnapshotAvailable();
                        logger.info("notify listener for snapshot id [" + listenSnapshotId + "]");
                    } catch (Exception e) {
                        logger.warn(
                                "trigger snapshotListener failed. snapshotId [" + snapshotId + "]");
                    }
                }
            }
            listenersToTrigger.clear();
        }
        return currentSnapshotId;
    }

    /**
     * Compiler use this method to get SnapshotInfo
     *
     * @return
     */
    public SnapshotWithSchema getSnapshotWithSchema() {
        return this.snapshotWithSchemaRef.get();
    }

    public void setGraphStatisticsRef(GraphStatistics statistics) {
        logger.info(
                "update graph statistics in frontend cache, vertex num: {}, edge num: {}",
                statistics.getVertexCount(),
                statistics.getEdgeCount());
        this.graphStatisticsRef.set(statistics);
    }

    public GraphStatistics getGraphStatistics() {
        return this.graphStatisticsRef.get();
    }
}
