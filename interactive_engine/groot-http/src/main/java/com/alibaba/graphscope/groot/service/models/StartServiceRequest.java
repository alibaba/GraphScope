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
 * StartServiceRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class StartServiceRequest {

  private String graphId;

  public StartServiceRequest graphId(String graphId) {
    this.graphId = graphId;
    return this;
  }

  /**
   * Get graphId
   * @return graphId
  */
  
  @Schema(name = "graph_id", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("graph_id")
  public String getGraphId() {
    return graphId;
  }

  public void setGraphId(String graphId) {
    this.graphId = graphId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StartServiceRequest startServiceRequest = (StartServiceRequest) o;
    return Objects.equals(this.graphId, startServiceRequest.graphId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(graphId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StartServiceRequest {\n");
    sb.append("    graphId: ").append(toIndentedString(graphId)).append("\n");
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

