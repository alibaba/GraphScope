package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.EdgeStatistics;
import com.alibaba.graphscope.groot.service.models.VertexStatistics;
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
 * GetGraphStatisticsResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetGraphStatisticsResponse {

  private Integer totalVertexCount;

  private Integer totalEdgeCount;

  @Valid
  private List<@Valid VertexStatistics> vertexTypeStatistics;

  @Valid
  private List<@Valid EdgeStatistics> edgeTypeStatistics;

  public GetGraphStatisticsResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public GetGraphStatisticsResponse(Integer totalVertexCount, Integer totalEdgeCount) {
    this.totalVertexCount = totalVertexCount;
    this.totalEdgeCount = totalEdgeCount;
  }

  public GetGraphStatisticsResponse totalVertexCount(Integer totalVertexCount) {
    this.totalVertexCount = totalVertexCount;
    return this;
  }

  /**
   * Get totalVertexCount
   * @return totalVertexCount
  */
  @NotNull 
  @Schema(name = "total_vertex_count", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("total_vertex_count")
  public Integer getTotalVertexCount() {
    return totalVertexCount;
  }

  public void setTotalVertexCount(Integer totalVertexCount) {
    this.totalVertexCount = totalVertexCount;
  }

  public GetGraphStatisticsResponse totalEdgeCount(Integer totalEdgeCount) {
    this.totalEdgeCount = totalEdgeCount;
    return this;
  }

  /**
   * Get totalEdgeCount
   * @return totalEdgeCount
  */
  @NotNull 
  @Schema(name = "total_edge_count", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("total_edge_count")
  public Integer getTotalEdgeCount() {
    return totalEdgeCount;
  }

  public void setTotalEdgeCount(Integer totalEdgeCount) {
    this.totalEdgeCount = totalEdgeCount;
  }

  public GetGraphStatisticsResponse vertexTypeStatistics(List<@Valid VertexStatistics> vertexTypeStatistics) {
    this.vertexTypeStatistics = vertexTypeStatistics;
    return this;
  }

  public GetGraphStatisticsResponse addVertexTypeStatisticsItem(VertexStatistics vertexTypeStatisticsItem) {
    if (this.vertexTypeStatistics == null) {
      this.vertexTypeStatistics = new ArrayList<>();
    }
    this.vertexTypeStatistics.add(vertexTypeStatisticsItem);
    return this;
  }

  /**
   * Get vertexTypeStatistics
   * @return vertexTypeStatistics
  */
  @Valid 
  @Schema(name = "vertex_type_statistics", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("vertex_type_statistics")
  public List<@Valid VertexStatistics> getVertexTypeStatistics() {
    return vertexTypeStatistics;
  }

  public void setVertexTypeStatistics(List<@Valid VertexStatistics> vertexTypeStatistics) {
    this.vertexTypeStatistics = vertexTypeStatistics;
  }

  public GetGraphStatisticsResponse edgeTypeStatistics(List<@Valid EdgeStatistics> edgeTypeStatistics) {
    this.edgeTypeStatistics = edgeTypeStatistics;
    return this;
  }

  public GetGraphStatisticsResponse addEdgeTypeStatisticsItem(EdgeStatistics edgeTypeStatisticsItem) {
    if (this.edgeTypeStatistics == null) {
      this.edgeTypeStatistics = new ArrayList<>();
    }
    this.edgeTypeStatistics.add(edgeTypeStatisticsItem);
    return this;
  }

  /**
   * Get edgeTypeStatistics
   * @return edgeTypeStatistics
  */
  @Valid 
  @Schema(name = "edge_type_statistics", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("edge_type_statistics")
  public List<@Valid EdgeStatistics> getEdgeTypeStatistics() {
    return edgeTypeStatistics;
  }

  public void setEdgeTypeStatistics(List<@Valid EdgeStatistics> edgeTypeStatistics) {
    this.edgeTypeStatistics = edgeTypeStatistics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetGraphStatisticsResponse getGraphStatisticsResponse = (GetGraphStatisticsResponse) o;
    return Objects.equals(this.totalVertexCount, getGraphStatisticsResponse.totalVertexCount) &&
        Objects.equals(this.totalEdgeCount, getGraphStatisticsResponse.totalEdgeCount) &&
        Objects.equals(this.vertexTypeStatistics, getGraphStatisticsResponse.vertexTypeStatistics) &&
        Objects.equals(this.edgeTypeStatistics, getGraphStatisticsResponse.edgeTypeStatistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(totalVertexCount, totalEdgeCount, vertexTypeStatistics, edgeTypeStatistics);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetGraphStatisticsResponse {\n");
    sb.append("    totalVertexCount: ").append(toIndentedString(totalVertexCount)).append("\n");
    sb.append("    totalEdgeCount: ").append(toIndentedString(totalEdgeCount)).append("\n");
    sb.append("    vertexTypeStatistics: ").append(toIndentedString(vertexTypeStatistics)).append("\n");
    sb.append("    edgeTypeStatistics: ").append(toIndentedString(edgeTypeStatistics)).append("\n");
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

