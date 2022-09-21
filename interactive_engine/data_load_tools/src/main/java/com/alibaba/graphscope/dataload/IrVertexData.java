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

import com.alibaba.graphscope.dataload.jna.type.FfiPropertyBytes;
import com.alibaba.graphscope.dataload.jna.type.FfiVertexData;
import com.alibaba.graphscope.dataload.jna.type.ResultCode;
import com.aliyun.odps.data.Record;
import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.util.Arrays;

public class IrVertexData implements RecordRWritable {
    public long id;
    public int primaryLabel;
    public int secondaryLabel;
    // property bytes
    public byte[] bytes;
    public int len;
    public int code;
    private Pointer propBuffer;

    public IrVertexData() {
        this.propBuffer = new Memory(IrDataBuild.PROP_BUFFER_SIZE);
    }

    public IrVertexData toIrVertexData(FfiVertexData.ByValue vertexData) {
        this.id = vertexData.id;
        this.primaryLabel = vertexData.primaryLabel;
        this.secondaryLabel = vertexData.secondaryLabel;
        this.bytes = vertexData.propertyBytes.getBytes();
        this.len = vertexData.propertyBytes.len;
        this.code = vertexData.code.getInt();
        return this;
    }

    public FfiVertexData.ByValue toFfiVertexData() {
        if (this.len > IrDataBuild.PROP_BUFFER_SIZE) {
            throw new RuntimeException(
                    "prop bytes in vertex data is out of length range, len is " + this.len);
        }
        this.propBuffer.write(0, this.bytes, 0, this.len);
        FfiVertexData.ByValue value =
                new FfiVertexData.ByValue(
                        this.id,
                        this.primaryLabel,
                        this.secondaryLabel,
                        new FfiPropertyBytes.ByValue(this.propBuffer, this.len),
                        ResultCode.Success.getEnum(code));
        return value;
    }

    @Override
    public void writeRecord(Record record) {
        record.setBigint(0, id);
        record.setBigint(1, Long.valueOf(primaryLabel));
        record.setBigint(2, Long.valueOf(secondaryLabel));
        // to keep the same schema with EdgeData
        record.setString(5, bytes);
        record.setBigint(6, Long.valueOf(len));
        record.setBigint(7, Long.valueOf(code));
    }

    @Override
    public void readRecord(Record record) {
        this.id = record.getBigint(0);
        this.primaryLabel = record.getBigint(1).intValue();
        this.secondaryLabel = record.getBigint(2).intValue();
        // to keep the same schema with EdgeData
        this.bytes = record.getBytes(5);
        this.len = record.getBigint(6).intValue();
        this.code = record.getBigint(7).intValue();
    }

    @Override
    public String toString() {
        return "IrVertexData{"
                + "id="
                + id
                + ", primaryLabel="
                + primaryLabel
                + ", secondaryLabel="
                + secondaryLabel
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
