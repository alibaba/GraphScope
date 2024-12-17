package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;
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
 * VertexEdgeRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VertexEdgeRequest {

  @Valid
  private List<@Valid VertexRequest> vertexRequest = new ArrayList<>();

  @Valid
  private List<@Valid EdgeRequest> edgeRequest = new ArrayList<>();

  public VertexEdgeRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public VertexEdgeRequest(List<@Valid VertexRequest> vertexRequest, List<@Valid EdgeRequest> edgeRequest) {
    this.vertexRequest = vertexRequest;
    this.edgeRequest = edgeRequest;
  }

  public VertexEdgeRequest vertexRequest(List<@Valid VertexRequest> vertexRequest) {
    this.vertexRequest = vertexRequest;
    return this;
  }

  public VertexEdgeRequest addVertexRequestItem(VertexRequest vertexRequestItem) {
    if (this.vertexRequest == null) {
      this.vertexRequest = new ArrayList<>();
    }
    this.vertexRequest.add(vertexRequestItem);
    return this;
  }

  /**
   * Get vertexRequest
   * @return vertexRequest
  */
  @NotNull @Valid 
  @Schema(name = "vertex_request", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("vertex_request")
  public List<@Valid VertexRequest> getVertexRequest() {
    return vertexRequest;
  }

  public void setVertexRequest(List<@Valid VertexRequest> vertexRequest) {
    this.vertexRequest = vertexRequest;
  }

  public VertexEdgeRequest edgeRequest(List<@Valid EdgeRequest> edgeRequest) {
    this.edgeRequest = edgeRequest;
    return this;
  }

  public VertexEdgeRequest addEdgeRequestItem(EdgeRequest edgeRequestItem) {
    if (this.edgeRequest == null) {
      this.edgeRequest = new ArrayList<>();
    }
    this.edgeRequest.add(edgeRequestItem);
    return this;
  }

  /**
   * Get edgeRequest
   * @return edgeRequest
  */
  @NotNull @Valid 
  @Schema(name = "edge_request", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("edge_request")
  public List<@Valid EdgeRequest> getEdgeRequest() {
    return edgeRequest;
  }

  public void setEdgeRequest(List<@Valid EdgeRequest> edgeRequest) {
    this.edgeRequest = edgeRequest;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexEdgeRequest vertexEdgeRequest = (VertexEdgeRequest) o;
    return Objects.equals(this.vertexRequest, vertexEdgeRequest.vertexRequest) &&
        Objects.equals(this.edgeRequest, vertexEdgeRequest.edgeRequest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vertexRequest, edgeRequest);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VertexEdgeRequest {\n");
    sb.append("    vertexRequest: ").append(toIndentedString(vertexRequest)).append("\n");
    sb.append("    edgeRequest: ").append(toIndentedString(edgeRequest)).append("\n");
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

