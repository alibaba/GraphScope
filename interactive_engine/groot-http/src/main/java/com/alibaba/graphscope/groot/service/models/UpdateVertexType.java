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
 * UpdateVertexType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-24T17:33:47.196892+08:00[Asia/Shanghai]")
public class UpdateVertexType {

  private String typeName;

  @Valid
  private List<String> primaryKeys;

  private BaseVertexTypeXCsrParams xCsrParams;

  @Valid
  private List<@Valid CreatePropertyMeta> propertiesToAdd;

  public UpdateVertexType typeName(String typeName) {
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

  public UpdateVertexType primaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
    return this;
  }

  public UpdateVertexType addPrimaryKeysItem(String primaryKeysItem) {
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

  public UpdateVertexType xCsrParams(BaseVertexTypeXCsrParams xCsrParams) {
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

  public UpdateVertexType propertiesToAdd(List<@Valid CreatePropertyMeta> propertiesToAdd) {
    this.propertiesToAdd = propertiesToAdd;
    return this;
  }

  public UpdateVertexType addPropertiesToAddItem(CreatePropertyMeta propertiesToAddItem) {
    if (this.propertiesToAdd == null) {
      this.propertiesToAdd = new ArrayList<>();
    }
    this.propertiesToAdd.add(propertiesToAddItem);
    return this;
  }

  /**
   * Get propertiesToAdd
   * @return propertiesToAdd
  */
  @Valid 
  @Schema(name = "properties_to_add", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("properties_to_add")
  public List<@Valid CreatePropertyMeta> getPropertiesToAdd() {
    return propertiesToAdd;
  }

  public void setPropertiesToAdd(List<@Valid CreatePropertyMeta> propertiesToAdd) {
    this.propertiesToAdd = propertiesToAdd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateVertexType updateVertexType = (UpdateVertexType) o;
    return Objects.equals(this.typeName, updateVertexType.typeName) &&
        Objects.equals(this.primaryKeys, updateVertexType.primaryKeys) &&
        Objects.equals(this.xCsrParams, updateVertexType.xCsrParams) &&
        Objects.equals(this.propertiesToAdd, updateVertexType.propertiesToAdd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, primaryKeys, xCsrParams, propertiesToAdd);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateVertexType {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    primaryKeys: ").append(toIndentedString(primaryKeys)).append("\n");
    sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
    sb.append("    propertiesToAdd: ").append(toIndentedString(propertiesToAdd)).append("\n");
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

