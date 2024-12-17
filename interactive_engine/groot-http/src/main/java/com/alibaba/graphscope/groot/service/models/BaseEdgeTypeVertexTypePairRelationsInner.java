package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * BaseEdgeTypeVertexTypePairRelationsInner
 */

@JsonTypeName("BaseEdgeType_vertex_type_pair_relations_inner")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class BaseEdgeTypeVertexTypePairRelationsInner {

  private String sourceVertex;

  private String destinationVertex;

  /**
   * Gets or Sets relation
   */
  public enum RelationEnum {
    MANY_TO_MANY("MANY_TO_MANY"),
    
    ONE_TO_MANY("ONE_TO_MANY"),
    
    MANY_TO_ONE("MANY_TO_ONE"),
    
    ONE_TO_ONE("ONE_TO_ONE");

    private String value;

    RelationEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static RelationEnum fromValue(String value) {
      for (RelationEnum b : RelationEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private RelationEnum relation;

  private BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams xCsrParams;

  public BaseEdgeTypeVertexTypePairRelationsInner sourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
    return this;
  }

  /**
   * Get sourceVertex
   * @return sourceVertex
  */
  
  @Schema(name = "source_vertex", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("source_vertex")
  public String getSourceVertex() {
    return sourceVertex;
  }

  public void setSourceVertex(String sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public BaseEdgeTypeVertexTypePairRelationsInner destinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
    return this;
  }

  /**
   * Get destinationVertex
   * @return destinationVertex
  */
  
  @Schema(name = "destination_vertex", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("destination_vertex")
  public String getDestinationVertex() {
    return destinationVertex;
  }

  public void setDestinationVertex(String destinationVertex) {
    this.destinationVertex = destinationVertex;
  }

  public BaseEdgeTypeVertexTypePairRelationsInner relation(RelationEnum relation) {
    this.relation = relation;
    return this;
  }

  /**
   * Get relation
   * @return relation
  */
  
  @Schema(name = "relation", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("relation")
  public RelationEnum getRelation() {
    return relation;
  }

  public void setRelation(RelationEnum relation) {
    this.relation = relation;
  }

  public BaseEdgeTypeVertexTypePairRelationsInner xCsrParams(BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams xCsrParams) {
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
  public BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams getxCsrParams() {
    return xCsrParams;
  }

  public void setxCsrParams(BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams xCsrParams) {
    this.xCsrParams = xCsrParams;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseEdgeTypeVertexTypePairRelationsInner baseEdgeTypeVertexTypePairRelationsInner = (BaseEdgeTypeVertexTypePairRelationsInner) o;
    return Objects.equals(this.sourceVertex, baseEdgeTypeVertexTypePairRelationsInner.sourceVertex) &&
        Objects.equals(this.destinationVertex, baseEdgeTypeVertexTypePairRelationsInner.destinationVertex) &&
        Objects.equals(this.relation, baseEdgeTypeVertexTypePairRelationsInner.relation) &&
        Objects.equals(this.xCsrParams, baseEdgeTypeVertexTypePairRelationsInner.xCsrParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceVertex, destinationVertex, relation, xCsrParams);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class BaseEdgeTypeVertexTypePairRelationsInner {\n");
    sb.append("    sourceVertex: ").append(toIndentedString(sourceVertex)).append("\n");
    sb.append("    destinationVertex: ").append(toIndentedString(destinationVertex)).append("\n");
    sb.append("    relation: ").append(toIndentedString(relation)).append("\n");
    sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
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

