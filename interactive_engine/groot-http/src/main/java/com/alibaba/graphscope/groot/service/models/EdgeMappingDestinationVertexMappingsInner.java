package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.EdgeMappingSourceVertexMappingsInnerColumn;
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
 * Mapping column to the primary key of destination vertex
 */

@Schema(name = "EdgeMapping_destination_vertex_mappings_inner", description = "Mapping column to the primary key of destination vertex")
@JsonTypeName("EdgeMapping_destination_vertex_mappings_inner")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeMappingDestinationVertexMappingsInner {

  private EdgeMappingSourceVertexMappingsInnerColumn column;

  private String property;

  public EdgeMappingDestinationVertexMappingsInner column(EdgeMappingSourceVertexMappingsInnerColumn column) {
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

  public EdgeMappingDestinationVertexMappingsInner property(String property) {
    this.property = property;
    return this;
  }

  /**
   * Get property
   * @return property
  */
  
  @Schema(name = "property", example = "id", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
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
    EdgeMappingDestinationVertexMappingsInner edgeMappingDestinationVertexMappingsInner = (EdgeMappingDestinationVertexMappingsInner) o;
    return Objects.equals(this.column, edgeMappingDestinationVertexMappingsInner.column) &&
        Objects.equals(this.property, edgeMappingDestinationVertexMappingsInner.property);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, property);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeMappingDestinationVertexMappingsInner {\n");
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

