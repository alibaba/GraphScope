package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * DateType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class DateType implements TemporalTypeTemporal {

  private String date32;

  public DateType() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public DateType(String date32) {
    this.date32 = date32;
  }

  public DateType date32(String date32) {
    this.date32 = date32;
    return this;
  }

  /**
   * Get date32
   * @return date32
  */
  @NotNull 
  @Schema(name = "date32", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("date32")
  public String getDate32() {
    return date32;
  }

  public void setDate32(String date32) {
    this.date32 = date32;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateType dateType = (DateType) o;
    return Objects.equals(this.date32, dateType.date32);
  }

  @Override
  public int hashCode() {
    return Objects.hash(date32);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DateType {\n");
    sb.append("    date32: ").append(toIndentedString(date32)).append("\n");
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

