package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.TemporalTypeTemporal;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * TemporalType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class TemporalType implements GSDataType {

  private TemporalTypeTemporal temporal;

  public TemporalType() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public TemporalType(TemporalTypeTemporal temporal) {
    this.temporal = temporal;
  }

  public TemporalType temporal(TemporalTypeTemporal temporal) {
    this.temporal = temporal;
    return this;
  }

  /**
   * Get temporal
   * @return temporal
  */
  @NotNull @Valid 
  @Schema(name = "temporal", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("temporal")
  public TemporalTypeTemporal getTemporal() {
    return temporal;
  }

  public void setTemporal(TemporalTypeTemporal temporal) {
    this.temporal = temporal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TemporalType temporalType = (TemporalType) o;
    return Objects.equals(this.temporal, temporalType.temporal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(temporal);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TemporalType {\n");
    sb.append("    temporal: ").append(toIndentedString(temporal)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

