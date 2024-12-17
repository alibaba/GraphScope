package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.GetEdgeType;
import com.alibaba.graphscope.groot.service.models.GetVertexType;
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
 * GetGraphSchemaResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetGraphSchemaResponse {

  @Valid
  private List<@Valid GetVertexType> vertexTypes;

  @Valid
  private List<@Valid GetEdgeType> edgeTypes;

  public GetGraphSchemaResponse vertexTypes(List<@Valid GetVertexType> vertexTypes) {
    this.vertexTypes = vertexTypes;
    return this;
  }

  public GetGraphSchemaResponse addVertexTypesItem(GetVertexType vertexTypesItem) {
    if (this.vertexTypes == null) {
      this.vertexTypes = new ArrayList<>();
    }
    this.vertexTypes.add(vertexTypesItem);
    return this;
  }

  /**
   * Get vertexTypes
   * @return vertexTypes
  */
  @Valid 
  @Schema(name = "vertex_types", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("vertex_types")
  public List<@Valid GetVertexType> getVertexTypes() {
    return vertexTypes;
  }

  public void setVertexTypes(List<@Valid GetVertexType> vertexTypes) {
    this.vertexTypes = vertexTypes;
  }

  public GetGraphSchemaResponse edgeTypes(List<@Valid GetEdgeType> edgeTypes) {
    this.edgeTypes = edgeTypes;
    return this;
  }

  public GetGraphSchemaResponse addEdgeTypesItem(GetEdgeType edgeTypesItem) {
    if (this.edgeTypes == null) {
      this.edgeTypes = new ArrayList<>();
    }
    this.edgeTypes.add(edgeTypesItem);
    return this;
  }

  /**
   * Get edgeTypes
   * @return edgeTypes
  */
  @Valid 
  @Schema(name = "edge_types", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("edge_types")
  public List<@Valid GetEdgeType> getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(List<@Valid GetEdgeType> edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetGraphSchemaResponse getGraphSchemaResponse = (GetGraphSchemaResponse) o;
    return Objects.equals(this.vertexTypes, getGraphSchemaResponse.vertexTypes) &&
        Objects.equals(this.edgeTypes, getGraphSchemaResponse.edgeTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vertexTypes, edgeTypes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetGraphSchemaResponse {\n");
    sb.append("    vertexTypes: ").append(toIndentedString(vertexTypes)).append("\n");
    sb.append("    edgeTypes: ").append(toIndentedString(edgeTypes)).append("\n");
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

