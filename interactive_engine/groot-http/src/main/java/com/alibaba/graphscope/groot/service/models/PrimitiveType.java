package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * PrimitiveType
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class PrimitiveType implements GSDataType {

  /**
   * Gets or Sets primitiveType
   */
  public enum PrimitiveTypeEnum {
    SIGNED_INT32("DT_SIGNED_INT32"),
    
    UNSIGNED_INT32("DT_UNSIGNED_INT32"),
    
    SIGNED_INT64("DT_SIGNED_INT64"),
    
    UNSIGNED_INT64("DT_UNSIGNED_INT64"),
    
    BOOL("DT_BOOL"),
    
    FLOAT("DT_FLOAT"),
    
    DOUBLE("DT_DOUBLE"),
    
    STRING("DT_STRING");

    private String value;

    PrimitiveTypeEnum(String value) {
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
    public static PrimitiveTypeEnum fromValue(String value) {
      for (PrimitiveTypeEnum b : PrimitiveTypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private PrimitiveTypeEnum primitiveType;

  public PrimitiveType() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public PrimitiveType(PrimitiveTypeEnum primitiveType) {
    this.primitiveType = primitiveType;
  }

  public PrimitiveType primitiveType(PrimitiveTypeEnum primitiveType) {
    this.primitiveType = primitiveType;
    return this;
  }

  /**
   * Get primitiveType
   * @return primitiveType
  */
  @NotNull 
  @Schema(name = "primitive_type", example = "DT_SIGNED_INT32", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("primitive_type")
  public PrimitiveTypeEnum getPrimitiveType() {
    return primitiveType;
  }

  public void setPrimitiveType(PrimitiveTypeEnum primitiveType) {
    this.primitiveType = primitiveType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveType primitiveType = (PrimitiveType) o;
    return Objects.equals(this.primitiveType, primitiveType.primitiveType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primitiveType);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class PrimitiveType {\n");
    sb.append("    primitiveType: ").append(toIndentedString(primitiveType)).append("\n");
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

