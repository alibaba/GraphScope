package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInner;
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
 * UpdateEdgeType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-24T17:33:47.196892+08:00[Asia/Shanghai]")
public class UpdateEdgeType {

  private String typeName;

  @Valid
  private List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> vertexTypePairRelations;

  @Valid
  private List<@Valid CreatePropertyMeta> propertiesToAdd;

  public UpdateEdgeType typeName(String typeName) {
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

  public UpdateEdgeType vertexTypePairRelations(List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> vertexTypePairRelations) {
    this.vertexTypePairRelations = vertexTypePairRelations;
    return this;
  }

  public UpdateEdgeType addVertexTypePairRelationsItem(BaseEdgeTypeVertexTypePairRelationsInner vertexTypePairRelationsItem) {
    if (this.vertexTypePairRelations == null) {
      this.vertexTypePairRelations = new ArrayList<>();
    }
    this.vertexTypePairRelations.add(vertexTypePairRelationsItem);
    return this;
  }

  /**
   * Get vertexTypePairRelations
   * @return vertexTypePairRelations
  */
  @Valid 
  @Schema(name = "vertex_type_pair_relations", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("vertex_type_pair_relations")
  public List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> getVertexTypePairRelations() {
    return vertexTypePairRelations;
  }

  public void setVertexTypePairRelations(List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> vertexTypePairRelations) {
    this.vertexTypePairRelations = vertexTypePairRelations;
  }

  public UpdateEdgeType propertiesToAdd(List<@Valid CreatePropertyMeta> propertiesToAdd) {
    this.propertiesToAdd = propertiesToAdd;
    return this;
  }

  public UpdateEdgeType addPropertiesToAddItem(CreatePropertyMeta propertiesToAddItem) {
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
    UpdateEdgeType updateEdgeType = (UpdateEdgeType) o;
    return Objects.equals(this.typeName, updateEdgeType.typeName) &&
        Objects.equals(this.vertexTypePairRelations, updateEdgeType.vertexTypePairRelations) &&
        Objects.equals(this.propertiesToAdd, updateEdgeType.propertiesToAdd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, vertexTypePairRelations, propertiesToAdd);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateEdgeType {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    vertexTypePairRelations: ").append(toIndentedString(vertexTypePairRelations)).append("\n");
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

