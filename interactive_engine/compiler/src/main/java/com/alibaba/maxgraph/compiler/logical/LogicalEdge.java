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
package com.alibaba.maxgraph.compiler.logical;

import com.alibaba.maxgraph.compiler.logical.edge.EdgeShuffleType;
import com.google.common.base.MoreObjects;

public class LogicalEdge {
    private EdgeShuffleType shuffleType;
    private long shuffleConstant;
    private int shufflePropId;
    private int streamIndex = 0;

    private LogicalEdge(EdgeShuffleType shuffleType,
                        long shuffleConstant,
                        int shufflePropId,
                        int streamIndex) {
        this.shuffleType = shuffleType;
        this.shuffleConstant = shuffleConstant;
        this.shufflePropId = shufflePropId;
        this.streamIndex = streamIndex;
    }

    public LogicalEdge() {
        this(EdgeShuffleType.SHUFFLE_BY_KEY);
    }

    public LogicalEdge(EdgeShuffleType shuffleType) {
        this.shuffleType = shuffleType;
        this.shuffleConstant = 0;
    }

    public EdgeShuffleType getShuffleType() {
        return shuffleType;
    }

    public long getShuffleConstant() {
        return shuffleConstant;
    }

    public int getShufflePropId() {
        return shufflePropId;
    }

    public void setStreamIndex(int streamIndex) {
        this.streamIndex = streamIndex;
    }

    public static LogicalEdge shuffleByKey(int shufflePropId) {
        return new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY, 0, shufflePropId, 0);
    }

    public static LogicalEdge forwardEdge() {
        return new LogicalEdge(EdgeShuffleType.FORWARD);
    }

    public static LogicalEdge shuffleConstant() {
        return new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST, 0, 0, 0);
    }

    public void setShuffleTypeForward() {
        this.shuffleType = EdgeShuffleType.FORWARD;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("shuffleType", shuffleType)
                .add("shuffleConstant", shuffleConstant)
                .toString();
    }

    public int getStreamIndex() {
        return streamIndex;
    }
}
