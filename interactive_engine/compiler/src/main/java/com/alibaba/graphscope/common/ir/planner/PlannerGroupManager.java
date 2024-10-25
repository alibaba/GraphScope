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

import com.alibaba.graphscope.common.config.PlannerConfig;
import com.google.common.base.Preconditions;

import org.apache.calcite.tools.RelBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class PlannerGroupManager implements Closeable {
    protected final PlannerConfig config;
    protected final RelBuilderFactory relBuilderFactory;

    public PlannerGroupManager(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
        this.config = config;
        this.relBuilderFactory = relBuilderFactory;
    }

    @Override
    public void close() {}

    public abstract PlannerGroup getCurrentGroup();

    public static class Static extends PlannerGroupManager {
        private final PlannerGroup singleGroup;

        public Static(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
            super(config, relBuilderFactory);
            this.singleGroup = new PlannerGroup(config, relBuilderFactory);
        }

        @Override
        public PlannerGroup getCurrentGroup() {
            return this.singleGroup;
        }
    }

    public static class Dynamic extends PlannerGroupManager {
        private final Logger logger = LoggerFactory.getLogger(PlannerGroupManager.class);
        private final List<PlannerGroup> plannerGroups;
        private final ScheduledExecutorService clearScheduler;

        public Dynamic(PlannerConfig config, RelBuilderFactory relBuilderFactory) {
            super(config, relBuilderFactory);
            Preconditions.checkArgument(
                    config.getPlannerGroupSize() > 0,
                    "planner group size should be greater than 0");
            this.plannerGroups = new ArrayList(config.getPlannerGroupSize());
            for (int i = 0; i < config.getPlannerGroupSize(); ++i) {
                this.plannerGroups.add(new PlannerGroup(config, relBuilderFactory));
            }
            this.clearScheduler = new ScheduledThreadPoolExecutor(1);
            int clearInterval = config.getPlannerGroupClearIntervalMinutes();
            this.clearScheduler.scheduleAtFixedRate(
                    () -> {
                        try {
                            long freeMemBytes = Runtime.getRuntime().freeMemory();
                            long totalMemBytes = Runtime.getRuntime().totalMemory();
                            Preconditions.checkArgument(
                                    totalMemBytes > 0, "total memory should be greater than 0");
                            if (freeMemBytes / (double) totalMemBytes < 0.2d) {
                                logger.warn(
                                        "start to clear planner groups. There are no enough memory"
                                                + " in JVM, with free memory: {}, total memory: {}",
                                        freeMemBytes,
                                        totalMemBytes);
                                plannerGroups.forEach(PlannerGroup::clear);
                            }
                        } catch (Throwable t) {
                            logger.error("failed to clear planner group.", t);
                        }
                    },
                    clearInterval,
                    clearInterval,
                    TimeUnit.MINUTES);
        }

        @Override
        public PlannerGroup getCurrentGroup() {
            Preconditions.checkArgument(
                    !plannerGroups.isEmpty(), "planner groups should not be empty");
            int groupId = (int) Thread.currentThread().getId() % plannerGroups.size();
            return plannerGroups.get(groupId);
        }

        @Override
        public void close() {
            try {
                if (this.clearScheduler != null) {
                    this.clearScheduler.shutdown();
                    this.clearScheduler.awaitTermination(10 * 1000, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                logger.error("failed to close planner group manager.", e);
            }
        }
    }
}
