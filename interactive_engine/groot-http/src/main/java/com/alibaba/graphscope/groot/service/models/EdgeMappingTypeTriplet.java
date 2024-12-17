package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
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
 * source label -&gt; [edge label] -&gt; destination label
 */

@Schema(name = "EdgeMapping_type_triplet", description = "source label -> [edge label] -> destination label")
@JsonTypeName("EdgeMapping_type_triplet")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeMappingTypeTriplet {

  private String edge;

  private String sourceVertex;

  private String destinationVertex;

  public EdgeMappingTypeTriplet edge(String edge) {
    this.edge = edge;
    return this;
  }

  /**
   * Get edge
   * @return edge
  */
  
  @Schema(name = "edge", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("edge")
  public String getEdge() {
    return edge;
  }

  public void setEdge(String edge) {
    this.edge = edge;
  }

  public EdgeMappingTypeTriplet sourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
    return this;
  }

  /**
   * Get sourceVertex
   * @return sourceVertex
  */
  
  @Schema(name = "source_vertex", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("source_vertex")
  public String getSourceVertex() {
    return sourceVertex;
  }

  public void setSourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public EdgeMappingTypeTriplet destinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
    return this;
  }

  /**
   * Get destinationVertex
   * @return destinationVertex
  */
  
  @Schema(name = "destination_vertex", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("destination_vertex")
  public String getDestinationVertex() {
    return destinationVertex;
  }

  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EdgeMappingTypeTriplet edgeMappingTypeTriplet = (EdgeMappingTypeTriplet) o;
    return Objects.equals(this.edge, edgeMappingTypeTriplet.edge) &&
        Objects.equals(this.sourceVertex, edgeMappingTypeTriplet.sourceVertex) &&
        Objects.equals(this.destinationVertex, edgeMappingTypeTriplet.destinationVertex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(edge, sourceVertex, destinationVertex);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeMappingTypeTriplet {\n");
    sb.append("    edge: ").append(toIndentedString(edge)).append("\n");
    sb.append("    sourceVertex: ").append(toIndentedString(sourceVertex)).append("\n");
    sb.append("    destinationVertex: ").append(toIndentedString(destinationVertex)).append("\n");
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

