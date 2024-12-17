package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.EdgeMapping;
import com.alibaba.graphscope.groot.service.models.SchemaMappingLoadingConfig;
import com.alibaba.graphscope.groot.service.models.VertexMapping;
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
 * SchemaMapping
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class SchemaMapping {

  private SchemaMappingLoadingConfig loadingConfig;

  @Valid
  private List<@Valid VertexMapping> vertexMappings;

  @Valid
  private List<@Valid EdgeMapping> edgeMappings;

  public SchemaMapping loadingConfig(SchemaMappingLoadingConfig loadingConfig) {
    this.loadingConfig = loadingConfig;
    return this;
  }

  /**
   * Get loadingConfig
   * @return loadingConfig
  */
  @Valid 
  @Schema(name = "loading_config", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("loading_config")
  public SchemaMappingLoadingConfig getLoadingConfig() {
    return loadingConfig;
  }

  public void setLoadingConfig(SchemaMappingLoadingConfig loadingConfig) {
    this.loadingConfig = loadingConfig;
  }

  public SchemaMapping vertexMappings(List<@Valid VertexMapping> vertexMappings) {
    this.vertexMappings = vertexMappings;
    return this;
  }

  public SchemaMapping addVertexMappingsItem(VertexMapping vertexMappingsItem) {
    if (this.vertexMappings == null) {
      this.vertexMappings = new ArrayList<>();
    }
    this.vertexMappings.add(vertexMappingsItem);
    return this;
  }

  /**
   * Get vertexMappings
   * @return vertexMappings
  */
  @Valid 
  @Schema(name = "vertex_mappings", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("vertex_mappings")
  public List<@Valid VertexMapping> getVertexMappings() {
    return vertexMappings;
  }

  public void setVertexMappings(List<@Valid VertexMapping> vertexMappings) {
    this.vertexMappings = vertexMappings;
  }

  public SchemaMapping edgeMappings(List<@Valid EdgeMapping> edgeMappings) {
    this.edgeMappings = edgeMappings;
    return this;
  }

  public SchemaMapping addEdgeMappingsItem(EdgeMapping edgeMappingsItem) {
    if (this.edgeMappings == null) {
      this.edgeMappings = new ArrayList<>();
    }
    this.edgeMappings.add(edgeMappingsItem);
    return this;
  }

  /**
   * Get edgeMappings
   * @return edgeMappings
  */
  @Valid 
  @Schema(name = "edge_mappings", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("edge_mappings")
  public List<@Valid EdgeMapping> getEdgeMappings() {
    return edgeMappings;
  }

  public void setEdgeMappings(List<@Valid EdgeMapping> edgeMappings) {
    this.edgeMappings = edgeMappings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaMapping schemaMapping = (SchemaMapping) o;
    return Objects.equals(this.loadingConfig, schemaMapping.loadingConfig) &&
        Objects.equals(this.vertexMappings, schemaMapping.vertexMappings) &&
        Objects.equals(this.edgeMappings, schemaMapping.edgeMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(loadingConfig, vertexMappings, edgeMappings);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaMapping {\n");
    sb.append("    loadingConfig: ").append(toIndentedString(loadingConfig)).append("\n");
    sb.append("    vertexMappings: ").append(toIndentedString(vertexMappings)).append("\n");
    sb.append("    edgeMappings: ").append(toIndentedString(edgeMappings)).append("\n");
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

