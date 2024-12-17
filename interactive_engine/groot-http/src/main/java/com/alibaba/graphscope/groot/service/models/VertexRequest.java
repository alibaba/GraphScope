package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Property;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
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
 * VertexRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VertexRequest {

  private String label;

  @Valid
  private List<@Valid Property> primaryKeyValues = new ArrayList<>();

  @Valid
  private List<@Valid Property> properties = new ArrayList<>();

  public VertexRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public VertexRequest(String label, List<@Valid Property> primaryKeyValues, List<@Valid Property> properties) {
    this.label = label;
    this.primaryKeyValues = primaryKeyValues;
    this.properties = properties;
  }

  public VertexRequest label(String label) {
    this.label = label;
    return this;
  }

  /**
   * Get label
   * @return label
  */
  @NotNull 
  @Schema(name = "label", example = "person", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public VertexRequest primaryKeyValues(List<@Valid Property> primaryKeyValues) {
    this.primaryKeyValues = primaryKeyValues;
    return this;
  }

  public VertexRequest addPrimaryKeyValuesItem(Property primaryKeyValuesItem) {
    if (this.primaryKeyValues == null) {
      this.primaryKeyValues = new ArrayList<>();
    }
    this.primaryKeyValues.add(primaryKeyValuesItem);
    return this;
  }

  /**
   * Get primaryKeyValues
   * @return primaryKeyValues
  */
  @NotNull @Valid 
  @Schema(name = "primary_key_values", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("primary_key_values")
  public List<@Valid Property> getPrimaryKeyValues() {
    return primaryKeyValues;
  }

  public void setPrimaryKeyValues(List<@Valid Property> primaryKeyValues) {
    this.primaryKeyValues = primaryKeyValues;
  }

  public VertexRequest properties(List<@Valid Property> properties) {
    this.properties = properties;
    return this;
  }

  public VertexRequest addPropertiesItem(Property propertiesItem) {
    if (this.properties == null) {
      this.properties = new ArrayList<>();
    }
    this.properties.add(propertiesItem);
    return this;
  }

  /**
   * Get properties
   * @return properties
  */
  @NotNull @Valid 
  @Schema(name = "properties", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("properties")
  public List<@Valid Property> getProperties() {
    return properties;
  }

  public void setProperties(List<@Valid Property> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexRequest vertexRequest = (VertexRequest) o;
    return Objects.equals(this.label, vertexRequest.label) &&
        Objects.equals(this.primaryKeyValues, vertexRequest.primaryKeyValues) &&
        Objects.equals(this.properties, vertexRequest.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, primaryKeyValues, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VertexRequest {\n");
    sb.append("    label: ").append(toIndentedString(label)).append("\n");
    sb.append("    primaryKeyValues: ").append(toIndentedString(primaryKeyValues)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
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

