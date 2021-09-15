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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TraversalId {
    private List<Integer> parentIds;

    public TraversalId(List<Integer> parentIds) {
        this.parentIds = parentIds;
    }

    public TraversalId(StepId stepId, int childId) {
        this.parentIds = new ArrayList<>(stepId.getTraversalId().parentIds);
        this.parentIds.add((childId << 16) | stepId.getStepId());
    }

    public List<Integer> getParentIds() {
        return parentIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraversalId that = (TraversalId) o;
        return Objects.equal(parentIds, that.parentIds);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(parentIds);
    }

    public TraversalId getParent() {
        if (parentIds.size() > 0) {
            return new TraversalId(parentIds.subList(0, parentIds.size() - 1));
        } else {
            return new TraversalId(Collections.EMPTY_LIST);
        }
    }

    public int depth() {
        return this.parentIds.size();
    }

    public static TraversalId root() {
        return new TraversalId(Collections.EMPTY_LIST);
    }

    public TraversalId fork(int stepId, int childId) {
        return new TraversalId(new StepId(this.getParent(), stepId), childId);
    }
}
