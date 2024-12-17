package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
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
 * Used for storage optimization
 */

@Schema(name = "BaseEdgeType_vertex_type_pair_relations_inner_x_csr_params", description = "Used for storage optimization")
@JsonTypeName("BaseEdgeType_vertex_type_pair_relations_inner_x_csr_params")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams {

  /**
   * Gets or Sets edgeStorageStrategy
   */
  public enum EdgeStorageStrategyEnum {
    ONLY_IN("ONLY_IN"),
    
    ONLY_OUT("ONLY_OUT"),
    
    BOTH_OUT_IN("BOTH_OUT_IN");

    private String value;

    EdgeStorageStrategyEnum(String value) {
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
    public static EdgeStorageStrategyEnum fromValue(String value) {
      for (EdgeStorageStrategyEnum b : EdgeStorageStrategyEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private EdgeStorageStrategyEnum edgeStorageStrategy;

  private Boolean sortOnCompaction;

  private String oeMutability;

  private String ieMutability;

  public BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams edgeStorageStrategy(EdgeStorageStrategyEnum edgeStorageStrategy) {
    this.edgeStorageStrategy = edgeStorageStrategy;
    return this;
  }

  /**
   * Get edgeStorageStrategy
   * @return edgeStorageStrategy
  */
  
  @Schema(name = "edge_storage_strategy", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("edge_storage_strategy")
  public EdgeStorageStrategyEnum getEdgeStorageStrategy() {
    return edgeStorageStrategy;
  }

  public void setEdgeStorageStrategy(EdgeStorageStrategyEnum edgeStorageStrategy) {
    this.edgeStorageStrategy = edgeStorageStrategy;
  }

  public BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams sortOnCompaction(Boolean sortOnCompaction) {
    this.sortOnCompaction = sortOnCompaction;
    return this;
  }

  /**
   * Get sortOnCompaction
   * @return sortOnCompaction
  */
  
  @Schema(name = "sort_on_compaction", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("sort_on_compaction")
  public Boolean getSortOnCompaction() {
    return sortOnCompaction;
  }

  public void setSortOnCompaction(Boolean sortOnCompaction) {
    this.sortOnCompaction = sortOnCompaction;
  }

  public BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams oeMutability(String oeMutability) {
    this.oeMutability = oeMutability;
    return this;
  }

  /**
   * Get oeMutability
   * @return oeMutability
  */
  
  @Schema(name = "oe_mutability", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("oe_mutability")
  public String getOeMutability() {
    return oeMutability;
  }

  public void setOeMutability(String oeMutability) {
    this.oeMutability = oeMutability;
  }

  public BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams ieMutability(String ieMutability) {
    this.ieMutability = ieMutability;
    return this;
  }

  /**
   * Get ieMutability
   * @return ieMutability
  */
  
  @Schema(name = "ie_mutability", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("ie_mutability")
  public String getIeMutability() {
    return ieMutability;
  }

  public void setIeMutability(String ieMutability) {
    this.ieMutability = ieMutability;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams baseEdgeTypeVertexTypePairRelationsInnerXCsrParams = (BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams) o;
    return Objects.equals(this.edgeStorageStrategy, baseEdgeTypeVertexTypePairRelationsInnerXCsrParams.edgeStorageStrategy) &&
        Objects.equals(this.sortOnCompaction, baseEdgeTypeVertexTypePairRelationsInnerXCsrParams.sortOnCompaction) &&
        Objects.equals(this.oeMutability, baseEdgeTypeVertexTypePairRelationsInnerXCsrParams.oeMutability) &&
        Objects.equals(this.ieMutability, baseEdgeTypeVertexTypePairRelationsInnerXCsrParams.ieMutability);
  }

  @Override
  public int hashCode() {
    return Objects.hash(edgeStorageStrategy, sortOnCompaction, oeMutability, ieMutability);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams {\n");
    sb.append("    edgeStorageStrategy: ").append(toIndentedString(edgeStorageStrategy)).append("\n");
    sb.append("    sortOnCompaction: ").append(toIndentedString(sortOnCompaction)).append("\n");
    sb.append("    oeMutability: ").append(toIndentedString(oeMutability)).append("\n");
    sb.append("    ieMutability: ").append(toIndentedString(ieMutability)).append("\n");
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

