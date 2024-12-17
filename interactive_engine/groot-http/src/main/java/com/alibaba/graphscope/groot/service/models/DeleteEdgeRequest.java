package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Property;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
 * DeleteEdgeRequest
 */

@JsonTypeName("delete_edge_request")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class DeleteEdgeRequest {

  @Valid
  private List<@Valid Property> srcPrimaryKeyValues;

  @Valid
  private List<@Valid Property> dstPrimaryKeyValues;

  public DeleteEdgeRequest srcPrimaryKeyValues(List<@Valid Property> srcPrimaryKeyValues) {
    this.srcPrimaryKeyValues = srcPrimaryKeyValues;
    return this;
  }

  public DeleteEdgeRequest addSrcPrimaryKeyValuesItem(Property srcPrimaryKeyValuesItem) {
    if (this.srcPrimaryKeyValues == null) {
      this.srcPrimaryKeyValues = new ArrayList<>();
    }
    this.srcPrimaryKeyValues.add(srcPrimaryKeyValuesItem);
    return this;
  }

  /**
   * Get srcPrimaryKeyValues
   * @return srcPrimaryKeyValues
  */
  @Valid 
  @Schema(name = "src_primary_key_values", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("src_primary_key_values")
  public List<@Valid Property> getSrcPrimaryKeyValues() {
    return srcPrimaryKeyValues;
  }

  public void setSrcPrimaryKeyValues(List<@Valid Property> srcPrimaryKeyValues) {
    this.srcPrimaryKeyValues = srcPrimaryKeyValues;
  }

  public DeleteEdgeRequest dstPrimaryKeyValues(List<@Valid Property> dstPrimaryKeyValues) {
    this.dstPrimaryKeyValues = dstPrimaryKeyValues;
    return this;
  }

  public DeleteEdgeRequest addDstPrimaryKeyValuesItem(Property dstPrimaryKeyValuesItem) {
    if (this.dstPrimaryKeyValues == null) {
      this.dstPrimaryKeyValues = new ArrayList<>();
    }
    this.dstPrimaryKeyValues.add(dstPrimaryKeyValuesItem);
    return this;
  }

  /**
   * Get dstPrimaryKeyValues
   * @return dstPrimaryKeyValues
  */
  @Valid 
  @Schema(name = "dst_primary_key_values", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("dst_primary_key_values")
  public List<@Valid Property> getDstPrimaryKeyValues() {
    return dstPrimaryKeyValues;
  }

  public void setDstPrimaryKeyValues(List<@Valid Property> dstPrimaryKeyValues) {
    this.dstPrimaryKeyValues = dstPrimaryKeyValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteEdgeRequest deleteEdgeRequest = (DeleteEdgeRequest) o;
    return Objects.equals(this.srcPrimaryKeyValues, deleteEdgeRequest.srcPrimaryKeyValues) &&
        Objects.equals(this.dstPrimaryKeyValues, deleteEdgeRequest.dstPrimaryKeyValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srcPrimaryKeyValues, dstPrimaryKeyValues);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteEdgeRequest {\n");
    sb.append("    srcPrimaryKeyValues: ").append(toIndentedString(srcPrimaryKeyValues)).append("\n");
    sb.append("    dstPrimaryKeyValues: ").append(toIndentedString(dstPrimaryKeyValues)).append("\n");
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

