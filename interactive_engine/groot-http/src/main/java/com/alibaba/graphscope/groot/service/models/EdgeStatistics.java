package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.VertexTypePairStatistics;
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
 * EdgeStatistics
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeStatistics {

  private Integer typeId;

  private String typeName;

  @Valid
  private List<@Valid VertexTypePairStatistics> vertexTypePairStatistics;

  public EdgeStatistics typeId(Integer typeId) {
    this.typeId = typeId;
    return this;
  }

  /**
   * Get typeId
   * @return typeId
  */
  
  @Schema(name = "type_id", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("type_id")
  public Integer getTypeId() {
    return typeId;
  }

  public void setTypeId(Integer typeId) {
    this.typeId = typeId;
  }

  public EdgeStatistics typeName(String typeName) {
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

  public EdgeStatistics vertexTypePairStatistics(List<@Valid VertexTypePairStatistics> vertexTypePairStatistics) {
    this.vertexTypePairStatistics = vertexTypePairStatistics;
    return this;
  }

  public EdgeStatistics addVertexTypePairStatisticsItem(VertexTypePairStatistics vertexTypePairStatisticsItem) {
    if (this.vertexTypePairStatistics == null) {
      this.vertexTypePairStatistics = new ArrayList<>();
    }
    this.vertexTypePairStatistics.add(vertexTypePairStatisticsItem);
    return this;
  }

  /**
   * Get vertexTypePairStatistics
   * @return vertexTypePairStatistics
  */
  @Valid 
  @Schema(name = "vertex_type_pair_statistics", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("vertex_type_pair_statistics")
  public List<@Valid VertexTypePairStatistics> getVertexTypePairStatistics() {
    return vertexTypePairStatistics;
  }

  public void setVertexTypePairStatistics(List<@Valid VertexTypePairStatistics> vertexTypePairStatistics) {
    this.vertexTypePairStatistics = vertexTypePairStatistics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EdgeStatistics edgeStatistics = (EdgeStatistics) o;
    return Objects.equals(this.typeId, edgeStatistics.typeId) &&
        Objects.equals(this.typeName, edgeStatistics.typeName) &&
        Objects.equals(this.vertexTypePairStatistics, edgeStatistics.vertexTypePairStatistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeId, typeName, vertexTypePairStatistics);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeStatistics {\n");
    sb.append("    typeId: ").append(toIndentedString(typeId)).append("\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    vertexTypePairStatistics: ").append(toIndentedString(vertexTypePairStatistics)).append("\n");
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

