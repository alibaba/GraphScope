package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
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
 * VertexTypePairStatistics
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VertexTypePairStatistics {

  private String sourceVertex;

  private String destinationVertex;

  private Integer count;

  public VertexTypePairStatistics() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public VertexTypePairStatistics(String sourceVertex, String destinationVertex, Integer count) {
    this.sourceVertex = sourceVertex;
    this.destinationVertex = destinationVertex;
    this.count = count;
  }

  public VertexTypePairStatistics sourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
    return this;
  }

  /**
   * Get sourceVertex
   * @return sourceVertex
  */
  @NotNull 
  @Schema(name = "source_vertex", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("source_vertex")
  public String getSourceVertex() {
    return sourceVertex;
  }

  public void setSourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public VertexTypePairStatistics destinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
    return this;
  }

  /**
   * Get destinationVertex
   * @return destinationVertex
  */
  @NotNull 
  @Schema(name = "destination_vertex", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("destination_vertex")
  public String getDestinationVertex() {
    return destinationVertex;
  }

  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }

  public VertexTypePairStatistics count(Integer count) {
    this.count = count;
    return this;
  }

  /**
   * Get count
   * @return count
  */
  @NotNull 
  @Schema(name = "count", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("count")
  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexTypePairStatistics vertexTypePairStatistics = (VertexTypePairStatistics) o;
    return Objects.equals(this.sourceVertex, vertexTypePairStatistics.sourceVertex) &&
        Objects.equals(this.destinationVertex, vertexTypePairStatistics.destinationVertex) &&
        Objects.equals(this.count, vertexTypePairStatistics.count);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceVertex, destinationVertex, count);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VertexTypePairStatistics {\n");
    sb.append("    sourceVertex: ").append(toIndentedString(sourceVertex)).append("\n");
    sb.append("    destinationVertex: ").append(toIndentedString(destinationVertex)).append("\n");
    sb.append("    count: ").append(toIndentedString(count)).append("\n");
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

