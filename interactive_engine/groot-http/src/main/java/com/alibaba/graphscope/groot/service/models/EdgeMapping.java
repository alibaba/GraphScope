package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.ColumnMapping;
import com.alibaba.graphscope.groot.service.models.EdgeMappingDestinationVertexMappingsInner;
import com.alibaba.graphscope.groot.service.models.EdgeMappingSourceVertexMappingsInner;
import com.alibaba.graphscope.groot.service.models.EdgeMappingTypeTriplet;
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
 * EdgeMapping
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeMapping {

  private EdgeMappingTypeTriplet typeTriplet;

  @Valid
  private List<String> inputs;

  @Valid
  private List<@Valid EdgeMappingSourceVertexMappingsInner> sourceVertexMappings;

  @Valid
  private List<@Valid EdgeMappingDestinationVertexMappingsInner> destinationVertexMappings;

  @Valid
  private List<@Valid ColumnMapping> columnMappings;

  public EdgeMapping typeTriplet(EdgeMappingTypeTriplet typeTriplet) {
    this.typeTriplet = typeTriplet;
    return this;
  }

  /**
   * Get typeTriplet
   * @return typeTriplet
  */
  @Valid 
  @Schema(name = "type_triplet", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("type_triplet")
  public EdgeMappingTypeTriplet getTypeTriplet() {
    return typeTriplet;
  }

  public void setTypeTriplet(EdgeMappingTypeTriplet typeTriplet) {
    this.typeTriplet = typeTriplet;
  }

  public EdgeMapping inputs(List<String> inputs) {
    this.inputs = inputs;
    return this;
  }

  public EdgeMapping addInputsItem(String inputsItem) {
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

  public EdgeMapping sourceVertexMappings(List<@Valid EdgeMappingSourceVertexMappingsInner> sourceVertexMappings) {
    this.sourceVertexMappings = sourceVertexMappings;
    return this;
  }

  public EdgeMapping addSourceVertexMappingsItem(EdgeMappingSourceVertexMappingsInner sourceVertexMappingsItem) {
    if (this.sourceVertexMappings == null) {
      this.sourceVertexMappings = new ArrayList<>();
    }
    this.sourceVertexMappings.add(sourceVertexMappingsItem);
    return this;
  }

  /**
   * Get sourceVertexMappings
   * @return sourceVertexMappings
  */
  @Valid 
  @Schema(name = "source_vertex_mappings", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("source_vertex_mappings")
  public List<@Valid EdgeMappingSourceVertexMappingsInner> getSourceVertexMappings() {
    return sourceVertexMappings;
  }

  public void setSourceVertexMappings(List<@Valid EdgeMappingSourceVertexMappingsInner> sourceVertexMappings) {
    this.sourceVertexMappings = sourceVertexMappings;
  }

  public EdgeMapping destinationVertexMappings(List<@Valid EdgeMappingDestinationVertexMappingsInner> destinationVertexMappings) {
    this.destinationVertexMappings = destinationVertexMappings;
    return this;
  }

  public EdgeMapping addDestinationVertexMappingsItem(EdgeMappingDestinationVertexMappingsInner destinationVertexMappingsItem) {
    if (this.destinationVertexMappings == null) {
      this.destinationVertexMappings = new ArrayList<>();
    }
    this.destinationVertexMappings.add(destinationVertexMappingsItem);
    return this;
  }

  /**
   * Get destinationVertexMappings
   * @return destinationVertexMappings
  */
  @Valid 
  @Schema(name = "destination_vertex_mappings", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("destination_vertex_mappings")
  public List<@Valid EdgeMappingDestinationVertexMappingsInner> getDestinationVertexMappings() {
    return destinationVertexMappings;
  }

  public void setDestinationVertexMappings(List<@Valid EdgeMappingDestinationVertexMappingsInner> destinationVertexMappings) {
    this.destinationVertexMappings = destinationVertexMappings;
  }

  public EdgeMapping columnMappings(List<@Valid ColumnMapping> columnMappings) {
    this.columnMappings = columnMappings;
    return this;
  }

  public EdgeMapping addColumnMappingsItem(ColumnMapping columnMappingsItem) {
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
    EdgeMapping edgeMapping = (EdgeMapping) o;
    return Objects.equals(this.typeTriplet, edgeMapping.typeTriplet) &&
        Objects.equals(this.inputs, edgeMapping.inputs) &&
        Objects.equals(this.sourceVertexMappings, edgeMapping.sourceVertexMappings) &&
        Objects.equals(this.destinationVertexMappings, edgeMapping.destinationVertexMappings) &&
        Objects.equals(this.columnMappings, edgeMapping.columnMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeTriplet, inputs, sourceVertexMappings, destinationVertexMappings, columnMappings);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeMapping {\n");
    sb.append("    typeTriplet: ").append(toIndentedString(typeTriplet)).append("\n");
    sb.append("    inputs: ").append(toIndentedString(inputs)).append("\n");
    sb.append("    sourceVertexMappings: ").append(toIndentedString(sourceVertexMappings)).append("\n");
    sb.append("    destinationVertexMappings: ").append(toIndentedString(destinationVertexMappings)).append("\n");
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

