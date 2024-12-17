package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Property;
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
 * EdgeRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeRequest {

  private String srcLabel;

  private String dstLabel;

  private String edgeLabel;

  @Valid
  private List<@Valid Property> srcPrimaryKeyValues = new ArrayList<>();

  @Valid
  private List<@Valid Property> dstPrimaryKeyValues = new ArrayList<>();

  @Valid
  private List<@Valid Property> properties;

  public EdgeRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public EdgeRequest(String srcLabel, String dstLabel, String edgeLabel, List<@Valid Property> srcPrimaryKeyValues, List<@Valid Property> dstPrimaryKeyValues) {
    this.srcLabel = srcLabel;
    this.dstLabel = dstLabel;
    this.edgeLabel = edgeLabel;
    this.srcPrimaryKeyValues = srcPrimaryKeyValues;
    this.dstPrimaryKeyValues = dstPrimaryKeyValues;
  }

  public EdgeRequest srcLabel(String srcLabel) {
    this.srcLabel = srcLabel;
    return this;
  }

  /**
   * Get srcLabel
   * @return srcLabel
  */
  @NotNull 
  @Schema(name = "src_label", example = "person", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("src_label")
  public String getSrcLabel() {
    return srcLabel;
  }

  public void setSrcLabel(String srcLabel) {
    this.srcLabel = srcLabel;
  }

  public EdgeRequest dstLabel(String dstLabel) {
    this.dstLabel = dstLabel;
    return this;
  }

  /**
   * Get dstLabel
   * @return dstLabel
  */
  @NotNull 
  @Schema(name = "dst_label", example = "software", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("dst_label")
  public String getDstLabel() {
    return dstLabel;
  }

  public void setDstLabel(String dstLabel) {
    this.dstLabel = dstLabel;
  }

  public EdgeRequest edgeLabel(String edgeLabel) {
    this.edgeLabel = edgeLabel;
    return this;
  }

  /**
   * Get edgeLabel
   * @return edgeLabel
  */
  @NotNull 
  @Schema(name = "edge_label", example = "created", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("edge_label")
  public String getEdgeLabel() {
    return edgeLabel;
  }

  public void setEdgeLabel(String edgeLabel) {
    this.edgeLabel = edgeLabel;
  }

  public EdgeRequest srcPrimaryKeyValues(List<@Valid Property> srcPrimaryKeyValues) {
    this.srcPrimaryKeyValues = srcPrimaryKeyValues;
    return this;
  }

  public EdgeRequest addSrcPrimaryKeyValuesItem(Property srcPrimaryKeyValuesItem) {
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
  @NotNull @Valid 
  @Schema(name = "src_primary_key_values", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("src_primary_key_values")
  public List<@Valid Property> getSrcPrimaryKeyValues() {
    return srcPrimaryKeyValues;
  }

  public void setSrcPrimaryKeyValues(List<@Valid Property> srcPrimaryKeyValues) {
    this.srcPrimaryKeyValues = srcPrimaryKeyValues;
  }

  public EdgeRequest dstPrimaryKeyValues(List<@Valid Property> dstPrimaryKeyValues) {
    this.dstPrimaryKeyValues = dstPrimaryKeyValues;
    return this;
  }

  public EdgeRequest addDstPrimaryKeyValuesItem(Property dstPrimaryKeyValuesItem) {
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
  @NotNull @Valid 
  @Schema(name = "dst_primary_key_values", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("dst_primary_key_values")
  public List<@Valid Property> getDstPrimaryKeyValues() {
    return dstPrimaryKeyValues;
  }

  public void setDstPrimaryKeyValues(List<@Valid Property> dstPrimaryKeyValues) {
    this.dstPrimaryKeyValues = dstPrimaryKeyValues;
  }

  public EdgeRequest properties(List<@Valid Property> properties) {
    this.properties = properties;
    return this;
  }

  public EdgeRequest addPropertiesItem(Property propertiesItem) {
    if (this.properties == null) {
      this.properties = new ArrayList<>();
    }
    this.properties.add(propertiesItem);
    return this;
  }

  /**
   * Get properties
   * @return properties
  */
  @Valid 
  @Schema(name = "properties", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("properties")
  public List<@Valid Property> getProperties() {
    return properties;
  }

  public void setProperties(List<@Valid Property> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EdgeRequest edgeRequest = (EdgeRequest) o;
    return Objects.equals(this.srcLabel, edgeRequest.srcLabel) &&
        Objects.equals(this.dstLabel, edgeRequest.dstLabel) &&
        Objects.equals(this.edgeLabel, edgeRequest.edgeLabel) &&
        Objects.equals(this.srcPrimaryKeyValues, edgeRequest.srcPrimaryKeyValues) &&
        Objects.equals(this.dstPrimaryKeyValues, edgeRequest.dstPrimaryKeyValues) &&
        Objects.equals(this.properties, edgeRequest.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srcLabel, dstLabel, edgeLabel, srcPrimaryKeyValues, dstPrimaryKeyValues, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeRequest {\n");
    sb.append("    srcLabel: ").append(toIndentedString(srcLabel)).append("\n");
    sb.append("    dstLabel: ").append(toIndentedString(dstLabel)).append("\n");
    sb.append("    edgeLabel: ").append(toIndentedString(edgeLabel)).append("\n");
    sb.append("    srcPrimaryKeyValues: ").append(toIndentedString(srcPrimaryKeyValues)).append("\n");
    sb.append("    dstPrimaryKeyValues: ").append(toIndentedString(dstPrimaryKeyValues)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
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

