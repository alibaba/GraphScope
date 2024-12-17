package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.BaseVertexTypeXCsrParams;
import com.alibaba.graphscope.groot.service.models.GetPropertyMeta;
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
 * GetVertexType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetVertexType {

  private String typeName;

  @Valid
  private List<String> primaryKeys;

  private BaseVertexTypeXCsrParams xCsrParams;

  private Integer typeId;

  @Valid
  private List<@Valid GetPropertyMeta> properties;

  private String description;

  public GetVertexType typeName(String typeName) {
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

  public GetVertexType primaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
    return this;
  }

  public GetVertexType addPrimaryKeysItem(String primaryKeysItem) {
    if (this.primaryKeys == null) {
      this.primaryKeys = new ArrayList<>();
    }
    this.primaryKeys.add(primaryKeysItem);
    return this;
  }

  /**
   * Get primaryKeys
   * @return primaryKeys
  */
  
  @Schema(name = "primary_keys", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("primary_keys")
  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public GetVertexType xCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
    this.xCsrParams = xCsrParams;
    return this;
  }

  /**
   * Get xCsrParams
   * @return xCsrParams
  */
  @Valid 
  @Schema(name = "x_csr_params", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("x_csr_params")
  public BaseVertexTypeXCsrParams getxCsrParams() {
    return xCsrParams;
  }

  public void setxCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
    this.xCsrParams = xCsrParams;
  }

  public GetVertexType typeId(Integer typeId) {
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

  public GetVertexType properties(List<@Valid GetPropertyMeta> properties) {
    this.properties = properties;
    return this;
  }

  public GetVertexType addPropertiesItem(GetPropertyMeta propertiesItem) {
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
  public List<@Valid GetPropertyMeta> getProperties() {
    return properties;
  }

  public void setProperties(List<@Valid GetPropertyMeta> properties) {
    this.properties = properties;
  }

  public GetVertexType description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * @return description
  */
  
  @Schema(name = "description", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetVertexType getVertexType = (GetVertexType) o;
    return Objects.equals(this.typeName, getVertexType.typeName) &&
        Objects.equals(this.primaryKeys, getVertexType.primaryKeys) &&
        Objects.equals(this.xCsrParams, getVertexType.xCsrParams) &&
        Objects.equals(this.typeId, getVertexType.typeId) &&
        Objects.equals(this.properties, getVertexType.properties) &&
        Objects.equals(this.description, getVertexType.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, primaryKeys, xCsrParams, typeId, properties, description);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetVertexType {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    primaryKeys: ").append(toIndentedString(primaryKeys)).append("\n");
    sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
    sb.append("    typeId: ").append(toIndentedString(typeId)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
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

