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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.output;


import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.io.Serializable;
import java.util.function.Function;

public class OutputOdpsFunction<IN> implements Function<Traverser<IN>, Long>, Serializable {
    private static final long serialVersionUID = -2502354835622595109L;
    private OutputOdpsTable outputOdpsTable;

    public OutputOdpsFunction(OutputOdpsTable outputOdpsTable) {
        this.outputOdpsTable = outputOdpsTable;
    }

    @Override
    public Long apply(Traverser<IN> traverser) {
        throw new NotImplementedException("apply");
    }

    public OutputOdpsTable getOutputOdpsTable() {
        return outputOdpsTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputOdpsFunction<?> that = (OutputOdpsFunction<?>) o;
        return Objects.equal(outputOdpsTable, that.outputOdpsTable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(outputOdpsTable);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .toString();
    }
}
