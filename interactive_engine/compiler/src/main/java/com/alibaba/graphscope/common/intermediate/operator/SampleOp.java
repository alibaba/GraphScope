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

package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.jna.type.FfiVariable;

import java.util.Objects;

public class SampleOp extends InterOpBase {
    private final SampleType sampleType;
    private final long seed;
    private final FfiVariable.ByValue variable;

    public SampleOp(SampleType sampleType, long seed, FfiVariable.ByValue variable) {
        super();
        this.sampleType = Objects.requireNonNull(sampleType);
        this.variable = Objects.requireNonNull(variable);
        this.seed = seed;
    }

    public SampleType getSampleType() {
        return sampleType;
    }

    public long getSeed() {
        return seed;
    }

    public FfiVariable.ByValue getVariable() {
        return variable;
    }

    public interface SampleType {}

    public static class RatioType implements SampleType {
        private final double ratio;

        public static RatioType create(double ratio) {
            return new RatioType(ratio);
        }

        private RatioType(double ratio) {
            this.ratio = ratio;
        }

        public double getRatio() {
            return ratio;
        }
    }

    public static class AmountType implements SampleType {
        private final long amount;

        public static AmountType create(long amount) {
            return new AmountType(amount);
        }

        private AmountType(long mount) {
            this.amount = mount;
        }

        public long getAmount() {
            return amount;
        }
    }
}
