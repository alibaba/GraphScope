package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.PrimitiveType;
import com.alibaba.graphscope.groot.service.models.StringType;
import com.alibaba.graphscope.groot.service.models.StringTypeString;
import com.alibaba.graphscope.groot.service.models.TemporalType;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;


@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME, 
  include = JsonTypeInfo.As.PROPERTY, 
  property = "type") 
@JsonSubTypes({
  @JsonSubTypes.Type(value = PrimitiveType.class, name = "PrimitiveType"), 
  @JsonSubTypes.Type(value = StringType.class, name = "StringType"), 
  @JsonSubTypes.Type(value = TemporalType.class, name = "TemporalType")
})
public interface GSDataType {
}
