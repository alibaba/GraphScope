package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Property;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * DeleteVertexRequest
 */

@JsonTypeName("delete_vertex_request")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-01-07T10:36:33.439661+08:00[Asia/Shanghai]")
public class DeleteVertexRequest {

  private String label;

  @Valid
  private List<@Valid Property> primaryKeyValues;

  public DeleteVertexRequest label(String label) {
    this.label = label;
    return this;
  }

  /**
   * The label name of the vertex to delete.
   * @return label
  */
  
  @Schema(name = "label", description = "The label name of the vertex to delete.", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public DeleteVertexRequest primaryKeyValues(List<@Valid Property> primaryKeyValues) {
    this.primaryKeyValues = primaryKeyValues;
    return this;
  }

  public DeleteVertexRequest addPrimaryKeyValuesItem(Property primaryKeyValuesItem) {
    if (this.primaryKeyValues == null) {
      this.primaryKeyValues = new ArrayList<>();
    }
    this.primaryKeyValues.add(primaryKeyValuesItem);
    return this;
  }

  /**
   * The primary key values for locating the vertex.
   * @return primaryKeyValues
  */
  @Valid 
  @Schema(name = "primary_key_values", description = "The primary key values for locating the vertex.", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("primary_key_values")
  public List<@Valid Property> getPrimaryKeyValues() {
    return primaryKeyValues;
  }

  public void setPrimaryKeyValues(List<@Valid Property> primaryKeyValues) {
    this.primaryKeyValues = primaryKeyValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteVertexRequest deleteVertexRequest = (DeleteVertexRequest) o;
    return Objects.equals(this.label, deleteVertexRequest.label) &&
        Objects.equals(this.primaryKeyValues, deleteVertexRequest.primaryKeyValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, primaryKeyValues);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteVertexRequest {\n");
    sb.append("    label: ").append(toIndentedString(label)).append("\n");
    sb.append("    primaryKeyValues: ").append(toIndentedString(primaryKeyValues)).append("\n");
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

