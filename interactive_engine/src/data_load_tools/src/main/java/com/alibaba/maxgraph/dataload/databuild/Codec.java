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
package com.alibaba.maxgraph.dataload.databuild;

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.graphscope.groot.schema.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Codec {
    private static final Logger logger = LoggerFactory.getLogger(Codec.class);

    private int version;
    private List<GraphProperty> propertyDefs;
    private List<Integer> offsets;
    private byte[] nullBytesHolder;
    private int fixedPropertiesCount;

    public Codec(GraphElement graphElement) {
        this.version = graphElement.getVersionId();

        List<GraphProperty> propertyList = graphElement.getPropertyList();
        propertyList.sort((p1, p2) -> {
            boolean p1Fix = p1.getDataType().isFixedLength();
            boolean p2Fix = p2.getDataType().isFixedLength();
            if (p1Fix ^ p2Fix) {
                if (p1Fix) {
                    return -1;
                } else {
                    return 1;
                }
            } else {
                return Integer.compare(p1.getId(), p2.getId());
            }
        });
        int nullBytesLen = (propertyList.size() + 7) / 8;
        this.nullBytesHolder = new byte[nullBytesLen];
        int offset = 4 + nullBytesLen;
        this.offsets = new ArrayList<>();
        this.offsets.add(offset);
        int fixedLenPropCount = 0;
        for (GraphProperty graphProperty : propertyList) {
            DataType dataType = graphProperty.getDataType();
            if (dataType.isFixedLength()) {
                fixedLenPropCount++;
                offset += dataType.getTypeLength();
                this.offsets.add(offset);
            }
        }

        this.propertyDefs = propertyList;
        this.fixedPropertiesCount = fixedLenPropCount;
    }

    public void encode(Map<Integer, PropertyValue> propertiesMap, ByteBuffer scratch) {
        scratch.putInt(this.version);
        scratch.put(this.nullBytesHolder);

        for (int i = 0; i < this.fixedPropertiesCount; i++) {
            GraphProperty propertyDef = this.propertyDefs.get(i);
            PropertyValue propertyValue = propertiesMap.get(propertyDef.getId());
            if (propertyValue != null) {
                writeBytes(scratch, this.offsets.get(i), propertyValue.getValBytes());
            } else if (propertyDef.getDefaultValue() != null) {
                PropertyValue defaultValue = new PropertyValue(propertyDef.getDataType(), propertyDef.getDefaultValue());
                writeBytes(scratch, this.offsets.get(i), defaultValue.getValBytes());
            } else {
                setNull(i, scratch);
            }
        }

        int varOffsetsPos = this.offsets.get(this.offsets.size() - 1);
        int dataOffset = varOffsetsPos + 3 * (this.propertyDefs.size() - this.fixedPropertiesCount);
        scratch.position(dataOffset);
        int varEndOffset = 0;
        for (int i = this.fixedPropertiesCount; i < this.propertyDefs.size(); i++) {
            GraphProperty propertyDef = this.propertyDefs.get(i);
            PropertyValue propertyValue = propertiesMap.get(propertyDef.getId());
            if (propertyValue != null) {
                byte[] valBytes = propertyValue.getValBytes();
                scratch.put(valBytes);
                varEndOffset += valBytes.length;
            } else if (propertyDef.getDefaultValue() != null) {
                PropertyValue defaultValue = new PropertyValue(propertyDef.getDataType(), propertyDef.getDefaultValue());
                byte[] valBytes = defaultValue.getValBytes();
                scratch.put(valBytes);
                varEndOffset += valBytes.length;
            } else {
                setNull(i, scratch);
            }
            byte[] endOffsetBytes = lengthBytes(varEndOffset);
            writeBytes(scratch, varOffsetsPos, endOffsetBytes);
            varOffsetsPos += 3;
        }
    }

    private void setNull(int idx, ByteBuffer output) {
        int byteOffset = idx / 8 + 4;
        int bitOffset = idx % 8;
        byte flag = (byte)(output.get(byteOffset) | (1 << (7 - bitOffset)));
        output.put(byteOffset, flag);
    }

    private void writeBytes(ByteBuffer scratch, int offset, byte[] valBytes) {
        for (int o = offset, b = 0; o < offset + valBytes.length; o++, b++) {
            scratch.put(o, valBytes[b]);
        }
    }

    private byte[] lengthBytes(int len) {
        byte[] res = new byte[3];
        res[0] = (byte) (len & 255);
        res[1] = (byte) (len >> 8 & 255);
        res[2] = (byte) (len >> 16);
        return res;
    }
}
