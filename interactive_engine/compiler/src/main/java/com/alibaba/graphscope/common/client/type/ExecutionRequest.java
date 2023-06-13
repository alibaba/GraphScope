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

package com.alibaba.graphscope.common.client.type;

import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;

/**
 * request to submit to remote engine service
 */
public class ExecutionRequest {
    private final long requestId;
    private final String requestName;
    private final LogicalPlan requestLogical;
    private final PhysicalBuilder requestPhysical;

    public ExecutionRequest(
            long requestId,
            String requestName,
            LogicalPlan requestLogical,
            PhysicalBuilder requestPhysical) {
        this.requestId = requestId;
        this.requestName = requestName;
        this.requestLogical = requestLogical;
        this.requestPhysical = requestPhysical;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getRequestName() {
        return requestName;
    }

    public LogicalPlan getRequestLogical() {
        return requestLogical;
    }

    public PhysicalBuilder getRequestPhysical() {
        return requestPhysical;
    }
}
