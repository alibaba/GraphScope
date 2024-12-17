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
 * EdgeData
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class EdgeData {

  private String srcLabel;

  private String dstLabel;

  private String edgeLabel;

  private JsonNullable<Object> srcPrimaryKeyValue = JsonNullable.<Object>undefined();

  private JsonNullable<Object> dstPrimaryKeyValue = JsonNullable.<Object>undefined();

  @Valid
  private List<@Valid Property> properties = new ArrayList<>();

  public EdgeData() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public EdgeData(String srcLabel, String dstLabel, String edgeLabel, Object srcPrimaryKeyValue, Object dstPrimaryKeyValue, List<@Valid Property> properties) {
    this.srcLabel = srcLabel;
    this.dstLabel = dstLabel;
    this.edgeLabel = edgeLabel;
    this.srcPrimaryKeyValue = JsonNullable.of(srcPrimaryKeyValue);
    this.dstPrimaryKeyValue = JsonNullable.of(dstPrimaryKeyValue);
    this.properties = properties;
  }

  public EdgeData srcLabel(String srcLabel) {
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

  public EdgeData dstLabel(String dstLabel) {
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

  public EdgeData edgeLabel(String edgeLabel) {
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

  public EdgeData srcPrimaryKeyValue(Object srcPrimaryKeyValue) {
    this.srcPrimaryKeyValue = JsonNullable.of(srcPrimaryKeyValue);
    return this;
  }

  /**
   * Get srcPrimaryKeyValue
   * @return srcPrimaryKeyValue
  */
  @NotNull 
  @Schema(name = "src_primary_key_value", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("src_primary_key_value")
  public JsonNullable<Object> getSrcPrimaryKeyValue() {
    return srcPrimaryKeyValue;
  }

  public void setSrcPrimaryKeyValue(JsonNullable<Object> srcPrimaryKeyValue) {
    this.srcPrimaryKeyValue = srcPrimaryKeyValue;
  }

  public EdgeData dstPrimaryKeyValue(Object dstPrimaryKeyValue) {
    this.dstPrimaryKeyValue = JsonNullable.of(dstPrimaryKeyValue);
    return this;
  }

  /**
   * Get dstPrimaryKeyValue
   * @return dstPrimaryKeyValue
  */
  @NotNull 
  @Schema(name = "dst_primary_key_value", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("dst_primary_key_value")
  public JsonNullable<Object> getDstPrimaryKeyValue() {
    return dstPrimaryKeyValue;
  }

  public void setDstPrimaryKeyValue(JsonNullable<Object> dstPrimaryKeyValue) {
    this.dstPrimaryKeyValue = dstPrimaryKeyValue;
  }

  public EdgeData properties(List<@Valid Property> properties) {
    this.properties = properties;
    return this;
  }

  public EdgeData addPropertiesItem(Property propertiesItem) {
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
  @NotNull @Valid 
  @Schema(name = "properties", requiredMode = Schema.RequiredMode.REQUIRED)
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
    EdgeData edgeData = (EdgeData) o;
    return Objects.equals(this.srcLabel, edgeData.srcLabel) &&
        Objects.equals(this.dstLabel, edgeData.dstLabel) &&
        Objects.equals(this.edgeLabel, edgeData.edgeLabel) &&
        Objects.equals(this.srcPrimaryKeyValue, edgeData.srcPrimaryKeyValue) &&
        Objects.equals(this.dstPrimaryKeyValue, edgeData.dstPrimaryKeyValue) &&
        Objects.equals(this.properties, edgeData.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(srcLabel, dstLabel, edgeLabel, srcPrimaryKeyValue, dstPrimaryKeyValue, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EdgeData {\n");
    sb.append("    srcLabel: ").append(toIndentedString(srcLabel)).append("\n");
    sb.append("    dstLabel: ").append(toIndentedString(dstLabel)).append("\n");
    sb.append("    edgeLabel: ").append(toIndentedString(edgeLabel)).append("\n");
    sb.append("    srcPrimaryKeyValue: ").append(toIndentedString(srcPrimaryKeyValue)).append("\n");
    sb.append("    dstPrimaryKeyValue: ").append(toIndentedString(dstPrimaryKeyValue)).append("\n");
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

