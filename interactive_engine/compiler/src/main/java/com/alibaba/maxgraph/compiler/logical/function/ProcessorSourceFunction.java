/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.logical.function;

import com.alibaba.maxgraph.Message;
import com.alibaba.maxgraph.QueryFlowOuterClass;

public class ProcessorSourceFunction extends ProcessorFunction {
    private QueryFlowOuterClass.OdpsQueryInput odpsQueryInput;

    public ProcessorSourceFunction(
            QueryFlowOuterClass.OperatorType operatorType,
            QueryFlowOuterClass.RangeLimit.Builder rangeLimit) {
        this(operatorType, null, rangeLimit);
    }

    public ProcessorSourceFunction(
            QueryFlowOuterClass.OperatorType operatorType,
            Message.Value.Builder argumentBuilder,
            QueryFlowOuterClass.RangeLimit.Builder rangeLimit) {
        super(operatorType, argumentBuilder, rangeLimit);
        this.odpsQueryInput = null;
    }

    public QueryFlowOuterClass.OdpsQueryInput getOdpsQueryInput() {
        return odpsQueryInput;
    }

    public void setOdpsQueryInput(QueryFlowOuterClass.OdpsQueryInput odpsQueryInput) {
        this.odpsQueryInput = odpsQueryInput;
    }

    public void resetOperatorType(QueryFlowOuterClass.OperatorType operatorType) {
        this.operatorType = operatorType;
    }
}
