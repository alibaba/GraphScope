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

package com.alibaba.graphscope.common.ir.planner.type;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;

import java.util.Objects;

public class EdgeDataKey implements DataKey {
    private final int lowOrderId;
    private final int highOrderId;
    private final PatternDirection direction;

    public EdgeDataKey(int orderId1, int orderId2, PatternDirection direction) {
        if (orderId1 <= orderId2) {
            this.lowOrderId = orderId1;
            this.highOrderId = orderId2;
            this.direction = direction;
        } else {
            this.lowOrderId = orderId2;
            this.highOrderId = orderId1;
            this.direction = revert(direction);
        }
    }

    public PatternDirection getDirection() {
        return direction;
    }

    public int getLowOrderId() {
        return lowOrderId;
    }

    public int getHighOrderId() {
        return highOrderId;
    }

    private PatternDirection revert(PatternDirection direction) {
        switch (direction) {
            case IN:
                return PatternDirection.OUT;
            case OUT:
                return PatternDirection.IN;
            case BOTH:
            default:
                return PatternDirection.BOTH;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeDataKey edgeKey = (EdgeDataKey) o;
        return lowOrderId == edgeKey.lowOrderId
                && highOrderId == edgeKey.highOrderId
                && direction == edgeKey.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowOrderId, highOrderId, direction);
    }

    @Override
    public String toString() {
        return "EdgeDataKey{"
                + "lowOrderId="
                + lowOrderId
                + ", highOrderId="
                + highOrderId
                + ", direction="
                + direction
                + '}';
    }
}
