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

package com.alibaba.graphscope.sdk.restful;

import com.alibaba.graphscope.sdk.GraphPlan;

import java.io.Serializable;
import java.util.Objects;

public class GraphPlanResponse implements Serializable {
    private GraphPlan graphPlan;
    private String errorMessage;

    public GraphPlanResponse() {}

    public GraphPlanResponse(GraphPlan graphPlan) {
        this.graphPlan = Objects.requireNonNull(graphPlan);
        this.errorMessage = null;
    }

    public GraphPlanResponse(String errorMessage) {
        this.graphPlan = null;
        this.errorMessage = Objects.requireNonNull(errorMessage);
    }

    public GraphPlan getGraphPlan() {
        return graphPlan;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setGraphPlan(GraphPlan graphPlan) {
        this.graphPlan = graphPlan;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
