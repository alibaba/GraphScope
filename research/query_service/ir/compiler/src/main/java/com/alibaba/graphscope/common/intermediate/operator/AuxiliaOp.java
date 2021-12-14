package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class AuxiliaOp extends InterOpBase {
    private Optional<OpArg> propertyDetails;

    public AuxiliaOp() {
        super();
        this.propertyDetails = Optional.empty();
    }

    public Optional<OpArg> getPropertyDetails() {
        return propertyDetails;
    }

    public void setPropertyDetails(OpArg propertyDetails) {
        this.propertyDetails = Optional.of(propertyDetails);
    }
}