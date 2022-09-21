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

package com.alibaba.graphscope.dataload.jna.type;

import com.alibaba.graphscope.dataload.jna.helper.IrTypeMapper;
import com.sun.jna.Structure;

@Structure.FieldOrder({
    "labelId",
    "srcVertexId",
    "srcLabelId",
    "dstVertexId",
    "dstLabelId",
    "propertyBytes",
    "code"
})
public class FfiEdgeData extends Structure {
    public FfiEdgeData() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiEdgeData implements Structure.ByValue {
        public ByValue() {}

        public ByValue(
                int labelId,
                long srcVertexId,
                int srcLabelId,
                long dstVertexId,
                int dstLabelId,
                FfiPropertyBytes.ByValue bytes,
                ResultCode code) {
            this.labelId = labelId;
            this.srcVertexId = srcVertexId;
            this.srcLabelId = srcLabelId;
            this.dstVertexId = dstVertexId;
            this.dstLabelId = dstLabelId;
            this.propertyBytes = bytes;
            this.code = code;
        }
    }

    public int labelId;
    public long srcVertexId;
    public int srcLabelId;
    public long dstVertexId;
    public int dstLabelId;
    public FfiPropertyBytes.ByValue propertyBytes;
    public ResultCode code;
}
