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
package com.alibaba.graphscope.gaia.plan.meta.object;

import com.google.common.base.Objects;

import java.util.Collections;

public class StepId {
    private TraversalId traversalId;
    private volatile Integer stepId;

    public StepId(TraversalId traversalId, Integer stepId) {
        this.traversalId = traversalId;
        this.stepId = stepId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StepId stepId1 = (StepId) o;
        return Objects.equal(traversalId, stepId1.traversalId) &&
                Objects.equal(stepId, stepId1.stepId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(traversalId, stepId);
    }

    public TraversalId getTraversalId() {
        return traversalId;
    }

    public Integer getStepId() {
        return stepId;
    }

    public void setStepId(Integer stepId) {
        this.stepId = stepId;
    }

    public static StepId KEEP_STEP_ID = new StepId(new TraversalId(Collections.EMPTY_LIST), 0);
}
