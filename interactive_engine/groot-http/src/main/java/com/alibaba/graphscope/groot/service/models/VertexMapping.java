package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.ColumnMapping;
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
 * VertexMapping
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VertexMapping {

  private String typeName;

  @Valid
  private List<String> inputs;

  @Valid
  private List<@Valid ColumnMapping> columnMappings;

  public VertexMapping typeName(String typeName) {
    this.typeName = typeName;
    return this;
  }

  /**
   * Get typeName
   * @return typeName
  */
  
  @Schema(name = "type_name", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("type_name")
  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public VertexMapping inputs(List<String> inputs) {
    this.inputs = inputs;
    return this;
  }

  public VertexMapping addInputsItem(String inputsItem) {
    if (this.inputs == null) {
      this.inputs = new ArrayList<>();
    }
    this.inputs.add(inputsItem);
    return this;
  }

  /**
   * Get inputs
   * @return inputs
  */
  
  @Schema(name = "inputs", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("inputs")
  public List<String> getInputs() {
    return inputs;
  }

  public void setInputs(List<String> inputs) {
    this.inputs = inputs;
  }

  public VertexMapping columnMappings(List<@Valid ColumnMapping> columnMappings) {
    this.columnMappings = columnMappings;
    return this;
  }

  public VertexMapping addColumnMappingsItem(ColumnMapping columnMappingsItem) {
    if (this.columnMappings == null) {
      this.columnMappings = new ArrayList<>();
    }
    this.columnMappings.add(columnMappingsItem);
    return this;
  }

  /**
   * Get columnMappings
   * @return columnMappings
  */
  @Valid 
  @Schema(name = "column_mappings", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("column_mappings")
  public List<@Valid ColumnMapping> getColumnMappings() {
    return columnMappings;
  }

  public void setColumnMappings(List<@Valid ColumnMapping> columnMappings) {
    this.columnMappings = columnMappings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexMapping vertexMapping = (VertexMapping) o;
    return Objects.equals(this.typeName, vertexMapping.typeName) &&
        Objects.equals(this.inputs, vertexMapping.inputs) &&
        Objects.equals(this.columnMappings, vertexMapping.columnMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, inputs, columnMappings);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VertexMapping {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    inputs: ").append(toIndentedString(inputs)).append("\n");
    sb.append("    columnMappings: ").append(toIndentedString(columnMappings)).append("\n");
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

