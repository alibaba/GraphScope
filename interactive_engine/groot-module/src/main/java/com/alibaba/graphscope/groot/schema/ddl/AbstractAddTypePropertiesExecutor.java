package com.alibaba.graphscope.groot.schema.ddl;

import com.alibaba.graphscope.groot.common.exception.DdlException;
import com.alibaba.graphscope.groot.common.schema.wrapper.*;
import com.alibaba.graphscope.groot.common.util.JSON;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.proto.groot.TypeDefPb;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractAddTypePropertiesExecutor extends AbstractDdlExecutor {

    private static final Logger logger =
            LoggerFactory.getLogger(AbstractAddTypePropertiesExecutor.class);

    private static final String NAME_REGEX = "^\\w{1,128}$";

    @Override
    public DdlResult execute(ByteString ddlBlob, GraphDef graphDef, int partitionCount)
            throws InvalidProtocolBufferException {
        TypeDefPb typeDefPb = TypeDefPb.parseFrom(ddlBlob);
        TypeDef incomingTypeDef = TypeDef.parseProto(typeDefPb);
        long version = graphDef.getSchemaVersion();
        String label = incomingTypeDef.getLabel();

        TypeDef previousTypeDef = graphDef.getTypeDef(label);
        if (previousTypeDef == null) {
            throw new DdlException(
                    "LabelName [" + label + "] cannot found exists label in Graph Def.");
        }
        if (previousTypeDef.getTypeEnum() != incomingTypeDef.getTypeEnum()) {
            throw new DdlException(
                    "LabelName ["
                            + label
                            + "] type enum has been change. origin type ["
                            + previousTypeDef.getTypeEnum()
                            + "].");
        }

        LabelId labelId = new LabelId(previousTypeDef.getLabelId());
        version++;
        if (!label.matches(NAME_REGEX)) {
            throw new DdlException("illegal label name [" + label + "]");
        }

        if (!graphDef.hasLabel(label)) {
            throw new DdlException(
                    "label [" + label + "] not found, schema version [" + version + "]");
        }

        if (incomingTypeDef.getTypeEnum() == TypeEnum.VERTEX) {
            if (this instanceof AddEdgeTypePropertiesExecutor) {
                throw new DdlException("Expect edge type but got vertex type");
            }
            if (incomingTypeDef.getPkIdxs().size() > 0) {
                throw new DdlException(
                        "Can not add primary key properties in exists Vertex type. label ["
                                + label
                                + "]");
            }
        } else {
            if (this instanceof AddVertexTypePropertiesExecutor) {
                throw new DdlException("Expect vertex type but got edge type");
            }
            if (incomingTypeDef.getPkIdxs().size() > 0) {
                throw new DdlException(
                        "Can not add primary key properties in exists Edge type. label ["
                                + label
                                + "]");
            }
        }

        GraphDef.Builder graphDefBuilder = GraphDef.newBuilder(graphDef);
        graphDefBuilder.setVersion(version);
        TypeDef.Builder typeDefBuilder = TypeDef.newBuilder(incomingTypeDef);
        typeDefBuilder.setVersionId((int) version);
        typeDefBuilder.setLabelId(labelId);

        int propertyIdx = graphDef.getPropertyIdx();
        Map<String, Integer> propertyNameToId = graphDef.getPropertyNameToId();
        List<PropertyDef> incomingProperties = incomingTypeDef.getProperties();
        checkDuplicatedPropertiesExists(incomingProperties);
        List<PropertyDef> previousProperties = previousTypeDef.getProperties();
        List<PropertyDef> allProperties =
                new ArrayList<>(previousProperties.size() + incomingProperties.size());
        List<PropertyDef> newIncomingProperties = new ArrayList<>(incomingProperties.size());
        allProperties.addAll(previousProperties);
        for (PropertyDef property : incomingProperties) {
            checkPropertiesExists(label, property, previousProperties);
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
            PropertyDef propertyDef =
                    PropertyDef.newBuilder(property)
                            .setId(propertyId)
                            .setInnerId(propertyId)
                            .build();
            allProperties.add(propertyDef);
            newIncomingProperties.add(propertyDef);
        }
        typeDefBuilder.setPropertyDefs(allProperties);
        graphDefBuilder.addTypeDef(typeDefBuilder.build());
        GraphDef newGraphDef = graphDefBuilder.build();

        typeDefBuilder.setPropertyDefs(newIncomingProperties);
        TypeDef newIncomingTypeDef = typeDefBuilder.build();

        List<Operation> operations = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            Operation operation = makeOperation(i, version, newIncomingTypeDef, newGraphDef);
            operations.add(operation);
        }
        logger.info("new incoming type def is {}", JSON.toJson(newIncomingTypeDef));
        return new DdlResult(newGraphDef, operations);
    }

    private void checkPropertiesExists(
            String label, PropertyDef property, List<PropertyDef> propertyDefs) {
        Integer propertyId = property.getId();
        String propertyName = property.getName();
        for (PropertyDef existsProperty : propertyDefs) {
            Integer curId = existsProperty.getId();
            String curName = existsProperty.getName();
            if (propertyName.equals(curName) || propertyId.equals(curId)) {
                throw new DdlException(
                        "propertyName ["
                                + propertyName
                                + "], propertyId ["
                                + propertyId
                                + "]"
                                + "already exists in Label ["
                                + label
                                + "].");
            }
        }
    }

    protected abstract Operation makeOperation(
            int partition, long version, TypeDef typeDef, GraphDef graphDef);
}
