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

import com.alibaba.graphscope.gaia.plan.extractor.PropertyExtractor;

import java.util.Optional;
import java.util.function.Function;

public class StepMetaRequiredInfo {
    private final boolean needPathHistory;
    private final Function<StepTraverserElement, TraverserElement> traverserMapFunc;
    private final Optional<PropertyExtractor> extractor;

    private StepMetaRequiredInfo(Builder builder) {
        this.needPathHistory = builder.needPathHistory;
        this.traverserMapFunc = builder.traverserMapFunc;
        this.extractor = Optional.ofNullable(builder.extractor);
    }

    public boolean isNeedPathHistory() {
        return needPathHistory;
    }

    public Function<StepTraverserElement, TraverserElement> getTraverserMapFunc() {
        return traverserMapFunc;
    }

    public Optional<PropertyExtractor> getExtractor() {
        return extractor;
    }

    public static class Builder {
        private boolean needPathHistory;
        private Function<StepTraverserElement, TraverserElement> traverserMapFunc;
        private PropertyExtractor extractor;

        public static Builder newBuilder() {
            return new Builder(false, Function.identity(), null);
        }

        private Builder(boolean needPathHistory, Function traverserMapFunc, PropertyExtractor extractor) {
            this.extractor = extractor;
            this.traverserMapFunc = traverserMapFunc;
            this.needPathHistory = needPathHistory;
        }

        public Builder setNeedPathHistory(boolean needPathHistory) {
            this.needPathHistory = needPathHistory;
            return this;
        }

        public Builder setTraverserMapFunc(Function<StepTraverserElement, TraverserElement> traverserMapFunc) {
            this.traverserMapFunc = traverserMapFunc;
            return this;
        }

        public Builder setExtractor(PropertyExtractor extractor) {
            this.extractor = extractor;
            return this;
        }

        public StepMetaRequiredInfo build() {
            return new StepMetaRequiredInfo(this);
        }
    }
}
