package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.GSDataType;
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
 * GetPropertyMeta
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetPropertyMeta {

  private String propertyName;

  private GSDataType propertyType;

  private Integer propertyId;

  public GetPropertyMeta propertyName(String propertyName) {
    this.propertyName = propertyName;
    return this;
  }

  /**
   * Get propertyName
   * @return propertyName
  */
  
  @Schema(name = "property_name", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("property_name")
  public String getPropertyName() {
    return propertyName;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  public GetPropertyMeta propertyType(GSDataType propertyType) {
    this.propertyType = propertyType;
    return this;
  }

  /**
   * Get propertyType
   * @return propertyType
  */
  @Valid 
  @Schema(name = "property_type", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("property_type")
  public GSDataType getPropertyType() {
    return propertyType;
  }

  public void setPropertyType(GSDataType propertyType) {
    this.propertyType = propertyType;
  }

  public GetPropertyMeta propertyId(Integer propertyId) {
    this.propertyId = propertyId;
    return this;
  }

  /**
   * Get propertyId
   * @return propertyId
  */
  
  @Schema(name = "property_id", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("property_id")
  public Integer getPropertyId() {
    return propertyId;
  }

  public void setPropertyId(Integer propertyId) {
    this.propertyId = propertyId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetPropertyMeta getPropertyMeta = (GetPropertyMeta) o;
    return Objects.equals(this.propertyName, getPropertyMeta.propertyName) &&
        Objects.equals(this.propertyType, getPropertyMeta.propertyType) &&
        Objects.equals(this.propertyId, getPropertyMeta.propertyId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propertyName, propertyType, propertyId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetPropertyMeta {\n");
    sb.append("    propertyName: ").append(toIndentedString(propertyName)).append("\n");
    sb.append("    propertyType: ").append(toIndentedString(propertyType)).append("\n");
    sb.append("    propertyId: ").append(toIndentedString(propertyId)).append("\n");
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

