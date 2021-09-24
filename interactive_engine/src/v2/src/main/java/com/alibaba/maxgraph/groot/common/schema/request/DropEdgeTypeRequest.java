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
package com.alibaba.maxgraph.groot.common.schema.request;

import com.alibaba.maxgraph.groot.common.operation.OperationType;
import com.alibaba.maxgraph.groot.common.schema.TypeDef;
import com.alibaba.maxgraph.groot.common.schema.TypeEnum;
import com.google.protobuf.ByteString;

public class DropEdgeTypeRequest extends AbstractDdlRequest {

    private String label;

    public DropEdgeTypeRequest(String label) {
        super(OperationType.DROP_EDGE_TYPE);
        this.label = label;
    }

    @Override
    protected ByteString getBytes() {
        return TypeDef.newBuilder().setTypeEnum(TypeEnum.EDGE).setLabel(label).build().toDdlProto().toByteString();
    }
}
