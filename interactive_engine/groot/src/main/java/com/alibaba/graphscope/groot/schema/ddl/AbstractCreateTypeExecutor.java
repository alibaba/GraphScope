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
package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.maxgraph.proto.groot.TypeDefPb;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.PropertyDef;
import com.alibaba.graphscope.groot.schema.TypeDef;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractCreateTypeExecutor extends AbstractDdlExecutor {

    private static final String NAME_REGEX = "^\\w{1,128}$";

    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        TypeDefPb typeDefPb = TypeDefPb.parseFrom(ddlBlob);
        TypeDef typeDef = TypeDef.parseProto(typeDefPb);
        long version = graphDef.getSchemaVersion();
        String label = typeDef.getLabel();

        if (!label.matches(NAME_REGEX)) {
            throw new DdlException("illegal label name [" + label + "]");
        }

        if (graphDef.hasLabel(label)) {
            throw new DdlException(
                    "label [" + label + "] already exists, schema version [" + version + "]");
        }

        if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
            if (this instanceof CreateEdgeTypeExecutor) {
                throw new DdlException("Expect edge type but got vertex type");
            }
            if (typeDef.getPkIdxs().size() == 0) {
                throw new DdlException(
                        "Vertex type must define primary key. label [" + label + "]");
            }
        } else if (this instanceof CreateVertexTypeExecutor) {
            throw new DdlException("Expect vertex type but got edge type");
        }

        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        TypeDef.Builder typeDefBuilder = TypeDef.newBuilder(typeDef);

        int propertyIdx = graphDef.getPropertyIdx();
        Map<String, Integer> propertyNameToId = graphDef.getPropertyNameToId();
        List<PropertyDef> inputPropertiesInfo = typeDef.getProperties();
        List<PropertyDef> propertyDefs = new ArrayList<>(inputPropertiesInfo.size());
        for (PropertyDef property : inputPropertiesInfo) {
            String propertyName = property.getName();
            if (!propertyName.matches(NAME_REGEX)) {
                throw new DdlException("illegal property name [" + propertyName + "]");
            }
            Integer propertyId = propertyNameToId.get(propertyName);
            if (propertyId == null) {
                propertyIdx++;
                propertyId = propertyIdx;
                graphDefBuilder.putPropertyNameToId(propertyName, propertyId);
                graphDefBuilder.setPropertyIdx(propertyIdx);
            }
            propertyDefs.add(
                    PropertyDef.newBuilder(property)
                            .setId(propertyId)
                            .setInnerId(propertyId)
                            .build());
        }
        typeDefBuilder.setPropertyDefs(propertyDefs);
        int labelIdx = graphDef.getLabelIdx() + 1;
        LabelId labelId = new LabelId(labelIdx);
        typeDefBuilder.setLabelId(labelId);
        TypeDef newTypeDef = typeDefBuilder.build();

        version++;
        graphDefBuilder.setVersion(version);
        graphDefBuilder.addTypeDef(newTypeDef);
        graphDefBuilder.setLabelIdx(labelIdx);

        if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
            long tableIdx = graphDef.getTableIdx();
            tableIdx++;
            graphDefBuilder.putVertexTableId(labelId, tableIdx);
            graphDefBuilder.setTableIdx(tableIdx);
        }
        GraphDef newGraphDef = graphDefBuilder.build();

        List<Operation> operations = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            Operation operation = makeOperation(i, version, newTypeDef, newGraphDef);
            operations.add(operation);
        }
        return new DdlResult(newGraphDef, operations);
    }

    protected abstract Operation makeOperation(
            int partition, long version, TypeDef typeDef, GraphDef graphDef);
}
