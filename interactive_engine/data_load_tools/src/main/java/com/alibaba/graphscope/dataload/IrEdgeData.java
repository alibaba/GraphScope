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

package com.alibaba.graphscope.dataload;

import com.alibaba.graphscope.dataload.jna.type.FfiEdgeData;
import com.alibaba.graphscope.dataload.jna.type.FfiPropertyBytes;
import com.alibaba.graphscope.dataload.jna.type.ResultCode;
import com.aliyun.odps.data.Record;
import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.util.Arrays;

public class IrEdgeData implements RecordRWritable {
    public int labelId;
    public long srcVertexId;
    public int srcLabelId;
    public long dstVertexId;
    public int dstLabelId;
    // property bytes
    public byte[] bytes;
    public int len;
    public int code;
    private Pointer propBuffer;

    public IrEdgeData() {
        this.propBuffer = new Memory(IrDataBuild.PROP_BUFFER_SIZE);
    }

    public IrEdgeData toIrEdgeData(FfiEdgeData.ByValue edgeData) {
        this.labelId = edgeData.labelId;
        this.srcVertexId = edgeData.srcVertexId;
        this.srcLabelId = edgeData.srcLabelId;
        this.dstVertexId = edgeData.dstVertexId;
        this.dstLabelId = edgeData.dstLabelId;
        this.bytes = edgeData.propertyBytes.getBytes();
        this.len = edgeData.propertyBytes.len;
        this.code = edgeData.code.getInt();
        return this;
    }

    public FfiEdgeData.ByValue toFfiEdgeData() {
        if (this.len > IrDataBuild.PROP_BUFFER_SIZE) {
            throw new RuntimeException(
                    "prop bytes in edge data is out of length range, len is " + this.len);
        }
        this.propBuffer.write(0, this.bytes, 0, this.len);
        return new FfiEdgeData.ByValue(
                this.labelId,
                this.srcVertexId,
                this.srcLabelId,
                this.dstVertexId,
                this.dstLabelId,
                new FfiPropertyBytes.ByValue(this.propBuffer, this.len),
                ResultCode.Success.getEnum(this.code));
    }

    @Override
    public void writeRecord(Record record) {
        record.setBigint(0, Long.valueOf(labelId));
        record.setBigint(1, srcVertexId);
        record.setBigint(2, Long.valueOf(srcLabelId));
        record.setBigint(3, dstVertexId);
        record.setBigint(4, Long.valueOf(dstLabelId));
        record.setString(5, bytes); // bytes content
        record.setBigint(6, Long.valueOf(len)); // bytes len
        record.setBigint(7, Long.valueOf(code));
    }

    @Override
    public void readRecord(Record record) {
        this.labelId = record.getBigint(0).intValue();
        this.srcVertexId = record.getBigint(1);
        this.srcLabelId = record.getBigint(2).intValue();
        this.dstVertexId = record.getBigint(3);
        this.dstLabelId = record.getBigint(4).intValue();
        this.bytes = record.getBytes(5);
        this.len = record.getBigint(6).intValue();
        this.code = record.getBigint(7).intValue();
    }

    @Override
    public String toString() {
        return "IrEdgeData{"
                + "labelId="
                + labelId
                + ", srcVertexId="
                + srcVertexId
                + ", srcLabelId="
                + srcLabelId
                + ", dstVertexId="
                + dstVertexId
                + ", dstLabelId="
                + dstLabelId
                + ", bytes="
                + Arrays.toString(bytes)
                + ", len="
                + len
                + ", code="
                + code
                + ", propBuffer="
                + propBuffer
                + '}';
    }
}
