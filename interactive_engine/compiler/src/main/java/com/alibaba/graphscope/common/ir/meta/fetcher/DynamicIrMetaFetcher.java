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
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.IrMetaUpdater;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.reader.IrMetaReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Periodically update IrMeta, with the update frequency controlled by configuration.
 * Specifically, for procedures, a remote update will be actively triggered when they are not found locally.
 */
public class DynamicIrMetaFetcher extends IrMetaFetcher implements IrMetaUpdater {
    private static final Logger logger = LoggerFactory.getLogger(DynamicIrMetaFetcher.class);
    private final ScheduledExecutorService scheduler;
    private volatile IrMeta currentState;

    public DynamicIrMetaFetcher(IrMetaReader dataReader, Configs configs) {
        super(dataReader);
        this.scheduler = new ScheduledThreadPoolExecutor(1);
        this.scheduler.scheduleAtFixedRate(
                () -> onUpdate(),
                0,
                FrontendConfig.IR_META_FETCH_INTERVAL_MS.get(configs),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<IrMeta> fetch() {
        return currentState == null ? Optional.empty() : Optional.of(currentState);
    }

    @Override
    public synchronized IrMeta onUpdate() {
        try {
            this.currentState = this.reader.readMeta();
            if (this.currentState != null) {
                GraphStoredProcedures procedures = this.currentState.getStoredProcedures();
                if (procedures != null) {
                    procedures.registerIrMetaUpdater(this);
                }
            }
        } catch (IOException e) {
            logger.warn("failed to update meta data, error is {}", e);
        }
        return this.currentState;
    }
}
