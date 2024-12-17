package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInner;
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
 * GetEdgeType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetEdgeType {

  private String typeName;

  @Valid
  private List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> vertexTypePairRelations;

  private Integer typeId;

  private String description;

  @Valid
  private List<@Valid GetPropertyMeta> properties;

  public GetEdgeType typeName(String typeName) {
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

  public GetEdgeType vertexTypePairRelations(List<@Valid BaseEdgeTypeVertexTypePairRelationsInner> vertexTypePairRelations) {
    this.vertexTypePairRelations = vertexTypePairRelations;
    return this;
  }

  public GetEdgeType addVertexTypePairRelationsItem(BaseEdgeTypeVertexTypePairRelationsInner vertexTypePairRelationsItem) {
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

  public GetEdgeType typeId(Integer typeId) {
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

  public GetEdgeType description(String description) {
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

  public GetEdgeType properties(List<@Valid GetPropertyMeta> properties) {
    this.properties = properties;
    return this;
  }

  public GetEdgeType addPropertiesItem(GetPropertyMeta propertiesItem) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetEdgeType getEdgeType = (GetEdgeType) o;
    return Objects.equals(this.typeName, getEdgeType.typeName) &&
        Objects.equals(this.vertexTypePairRelations, getEdgeType.vertexTypePairRelations) &&
        Objects.equals(this.typeId, getEdgeType.typeId) &&
        Objects.equals(this.description, getEdgeType.description) &&
        Objects.equals(this.properties, getEdgeType.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, vertexTypePairRelations, typeId, description, properties);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetEdgeType {\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    vertexTypePairRelations: ").append(toIndentedString(vertexTypePairRelations)).append("\n");
    sb.append("    typeId: ").append(toIndentedString(typeId)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
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

