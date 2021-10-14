/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.component;

import com.alibaba.maxgraph.common.cluster.InstanceConfig;

import java.io.IOException;

public abstract class AbstractLifecycleComponent extends AbstractComponent implements LifecycleComponent {
    protected AbstractLifecycleComponent(InstanceConfig settings) {
        super(settings);
    }

    protected final Lifecycle lifecycle = new Lifecycle();

    @Override
    public void start() {
        if (!lifecycle.canMoveToStarted()) {
            return;
        }
        doStart();
        lifecycle.moveToStarted();
    }

    protected abstract void doStart();

    @Override
    public void stop() {
        if (!lifecycle.canMoveToStopped()) {
            return;
        }
        lifecycle.moveToStopped();
        doStop();
    }

    protected abstract void doStop();

    @Override
    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.canMoveToClosed()) {
            return;
        }
        lifecycle.moveToClosed();
        try {
            doClose();
        } catch (IOException e) {
            logger.warn("failed to close " + getClass().getName(), e);
        }
    }

    protected abstract void doClose() throws IOException;

}
