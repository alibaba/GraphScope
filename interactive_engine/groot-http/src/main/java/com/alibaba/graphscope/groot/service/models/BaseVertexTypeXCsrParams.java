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
 * Used for storage optimization
 */

@Schema(name = "BaseVertexType_x_csr_params", description = "Used for storage optimization")
@JsonTypeName("BaseVertexType_x_csr_params")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class BaseVertexTypeXCsrParams {

  private Integer maxVertexNum;

  public BaseVertexTypeXCsrParams maxVertexNum(Integer maxVertexNum) {
    this.maxVertexNum = maxVertexNum;
    return this;
  }

  /**
   * Get maxVertexNum
   * @return maxVertexNum
  */
  
  @Schema(name = "max_vertex_num", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("max_vertex_num")
  public Integer getMaxVertexNum() {
    return maxVertexNum;
  }

  public void setMaxVertexNum(Integer maxVertexNum) {
    this.maxVertexNum = maxVertexNum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseVertexTypeXCsrParams baseVertexTypeXCsrParams = (BaseVertexTypeXCsrParams) o;
    return Objects.equals(this.maxVertexNum, baseVertexTypeXCsrParams.maxVertexNum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxVertexNum);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class BaseVertexTypeXCsrParams {\n");
    sb.append("    maxVertexNum: ").append(toIndentedString(maxVertexNum)).append("\n");
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

