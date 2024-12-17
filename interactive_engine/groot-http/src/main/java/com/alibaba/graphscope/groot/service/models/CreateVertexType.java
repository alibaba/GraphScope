package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.BaseVertexTypeXCsrParams;
import com.alibaba.graphscope.groot.service.models.CreatePropertyMeta;
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
 * CreateVertexType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class CreateVertexType {

  private String typeName;

  @Valid
  private List<String> primaryKeys;

  private BaseVertexTypeXCsrParams xCsrParams;

  @Valid
  private List<@Valid CreatePropertyMeta> properties;

  public CreateVertexType typeName(String typeName) {
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

  public CreateVertexType primaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
    return this;
  }

  public CreateVertexType addPrimaryKeysItem(String primaryKeysItem) {
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

  public CreateVertexType xCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
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

  public CreateVertexType properties(List<@Valid CreatePropertyMeta> properties) {
    this.properties = properties;
    return this;
  }

  public CreateVertexType addPropertiesItem(CreatePropertyMeta propertiesItem) {
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
  public List<@Valid CreatePropertyMeta> getProperties() {
    return properties;
  }

  public void setProperties(List<@Valid CreatePropertyMeta> properties) {
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
    CreateVertexType createVertexType = (CreateVertexType) o;
    return Objects.equals(this.typeName, createVertexType.typeName) &&
        Objects.equals(this.primaryKeys, createVertexType.primaryKeys) &&
        Objects.equals(this.xCsrParams, createVertexType.xCsrParams) &&
        Objects.equals(this.properties, createVertexType.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, primaryKeys, xCsrParams, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateVertexType {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    primaryKeys: ").append(toIndentedString(primaryKeys)).append("\n");
    sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
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

