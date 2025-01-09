package com.alibaba.graphscope.groot.service.models;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.*;

import javax.validation.constraints.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = PrimitiveType.class, name = "PrimitiveType"),
    @JsonSubTypes.Type(value = StringType.class, name = "StringType"),
    @JsonSubTypes.Type(value = TemporalType.class, name = "TemporalType")
})
public interface GSDataType {}
