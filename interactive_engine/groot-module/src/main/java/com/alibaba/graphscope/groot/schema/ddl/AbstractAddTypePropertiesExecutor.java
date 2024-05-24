package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.common.schema.wrapper.*;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.alibaba.graphscope.proto.groot.TypeDefPb;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractAddTypePropertiesExecutor extends AbstractDdlExecutor {

    private static final String NAME_REGEX = "^\\w{1,128}$";

    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        TypeDefPb typeDefPb = TypeDefPb.parseFrom(ddlBlob);
        TypeDef typeDef = TypeDef.parseProto(typeDefPb);
        long version = graphDef.getSchemaVersion();
        String label = typeDef.getLabel();
        LabelId labelId = typeDef.getTypeLabelId();

        if (labelId == null) {
            throw new DdlException("type label id is NULL");
        }

        if (labelId.getId() < 0) {
            throw new DdlException("illegal label id [" + labelId.getId() + "]");
        }

        if (!label.matches(NAME_REGEX)) {
            throw new DdlException("illegal label name [" + label + "]");
        }

        if (!graphDef.hasLabel(label)) {
            throw new DdlException(
                    "label [" + label + "] not found, schema version [" + version + "]");
        }

        if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
            if (this instanceof AddEdgeTypePropertiesExecutor) {
                throw new DdlException("Expect edge type but got vertex type");
            }
            if (typeDef.getPkIdxs().size() > 0) {
                throw new DdlException(
                        "Can not add primary key properties in exists Vertex type. label [" + label + "]");
            }
        } else {
            if (this instanceof AddVertexTypePropertiesExecutor) {
                throw new DdlException("Expect vertex type but got edge type");
            }
            if (typeDef.getPkIdxs().size() > 0) {
                throw new DdlException(
                        "Can not add primary key properties in exists Edge type. label [" + label + "]");
            }
        }

        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        TypeDef originTypeDef = graphDef.getTypeDef(labelId);

        if (originTypeDef == null) {
            throw new DdlException("LabelName [" + label + "] can not found exists label in Graph Def.");
        }
        if (originTypeDef.getTypeEnum() != typeDef.getTypeEnum()) {
            throw new DdlException("LabelName [" + label + "] type enum has been change. origin type ["
                    + originTypeDef.getTypeEnum() + "].");
        }
        TypeDef.Builder newTypeDefBuilder = TypeDef.newBuilder(typeDef);

        int propertyIdx = graphDef.getPropertyIdx();
        Map<String, Integer> propertyNameToId = graphDef.getPropertyNameToId();
        List<PropertyDef> inputPropertiesInfo = typeDef.getProperties();
        List<PropertyDef> existsPropertiesInfo = originTypeDef.getProperties();
        List<PropertyDef> originPropertyDefs = new ArrayList<>(existsPropertiesInfo.size() + inputPropertiesInfo.size());
        List<PropertyDef> newPropertyDefs = new ArrayList<>(inputPropertiesInfo.size());
        for (PropertyDef existsProperty : existsPropertiesInfo) {
            originPropertyDefs.add(existsProperty);
        }
        for (PropertyDef property : inputPropertiesInfo) {
            Integer propertyId = property.getId();
            String propertyName = property.getName();
            if (!propertyName.matches(NAME_REGEX)) {
                throw new DdlException("illegal property name [" + propertyName + "]");
            }
            checkPropertiesExists(label, propertyId, propertyName, existsPropertiesInfo);
            propertyId = propertyNameToId.get(propertyName);
            if (propertyId == null) {
                propertyIdx++;
                propertyId = propertyIdx;
                graphDefBuilder.putPropertyNameToId(propertyName, propertyId);
                graphDefBuilder.setPropertyIdx(propertyIdx);
            }
            originPropertyDefs.add(
                    PropertyDef.newBuilder(property)
                            .setId(propertyId)
                            .setInnerId(propertyId)
                            .build());
            newPropertyDefs.add(
                    PropertyDef.newBuilder(property)
                            .setId(propertyId)
                            .setInnerId(propertyId)
                            .build());
        }
        newTypeDefBuilder.setPropertyDefs(originPropertyDefs);
        TypeDef originNewTypeDef = newTypeDefBuilder.build();

        newTypeDefBuilder.setPropertyDefs(newPropertyDefs);
        TypeDef newTypeDef = newTypeDefBuilder.build();

        if (newTypeDef.getLabelId() > graphDef.getLabelIdx()) {
            throw new DdlException("illegal label id [" + newTypeDef.getLabelId() + "], " +
                    "large than labelIdx [" + graphDef.getLabelIdx() + "]");
        }

        version++;
        graphDefBuilder.setVersion(version);
        graphDefBuilder.addTypeDef(originNewTypeDef);

        if (typeDef.getTypeEnum() == TypeEnum.VERTEX) {
            long tableIdx = graphDef.getTableIdx();
            tableIdx++;
            graphDefBuilder.putVertexTableId(newTypeDef.getTypeLabelId(), tableIdx);
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

    private void checkPropertiesExists(String label, Integer propertyId, String propertyName,
                                       List<PropertyDef> propertyDefs) {
        for (PropertyDef existsProperty : propertyDefs) {
            if (propertyName.equals(existsProperty.getName())
                    || (propertyId != null && propertyId == existsProperty.getId())) {
                throw new DdlException("propertyName [" + propertyName + "], propertyId [" + propertyId + "]" +
                        "already exists in Label [" + label + "].");
            }
        }
    }

    protected abstract Operation makeOperation(
            int partition, long version, TypeDef typeDef, GraphDef graphDef);
}
