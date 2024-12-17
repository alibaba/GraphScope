package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.EdgeMappingSourceVertexMappingsInnerColumn;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * ColumnMapping
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class ColumnMapping {

  private EdgeMappingSourceVertexMappingsInnerColumn column;

  private String property;

  public ColumnMapping column(EdgeMappingSourceVertexMappingsInnerColumn column) {
    this.column = column;
    return this;
  }

  /**
   * Get column
   * @return column
  */
  @Valid 
  @Schema(name = "column", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("column")
  public EdgeMappingSourceVertexMappingsInnerColumn getColumn() {
    return column;
  }

  public void setColumn(EdgeMappingSourceVertexMappingsInnerColumn column) {
    this.column = column;
  }

  public ColumnMapping property(String property) {
    this.property = property;
    return this;
  }

  /**
   * must align with the schema
   * @return property
  */
  
  @Schema(name = "property", description = "must align with the schema", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("property")
  public String getProperty() {
    return property;
  }

  public void setProperty(String property) {
    this.property = property;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnMapping columnMapping = (ColumnMapping) o;
    return Objects.equals(this.column, columnMapping.column) &&
        Objects.equals(this.property, columnMapping.property);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, property);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ColumnMapping {\n");
    sb.append("    column: ").append(toIndentedString(column)).append("\n");
    sb.append("    property: ").append(toIndentedString(property)).append("\n");
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

