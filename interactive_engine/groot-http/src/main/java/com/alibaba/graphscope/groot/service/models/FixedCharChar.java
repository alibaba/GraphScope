package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * FixedCharChar
 */

@JsonTypeName("FixedChar_char")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class FixedCharChar {

  private Integer fixedLength;

  public FixedCharChar() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public FixedCharChar(Integer fixedLength) {
    this.fixedLength = fixedLength;
  }

  public FixedCharChar fixedLength(Integer fixedLength) {
    this.fixedLength = fixedLength;
    return this;
  }

  /**
   * Get fixedLength
   * @return fixedLength
  */
  @NotNull 
  @Schema(name = "fixed_length", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("fixed_length")
  public Integer getFixedLength() {
    return fixedLength;
  }

  public void setFixedLength(Integer fixedLength) {
    this.fixedLength = fixedLength;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FixedCharChar fixedCharChar = (FixedCharChar) o;
    return Objects.equals(this.fixedLength, fixedCharChar.fixedLength);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fixedLength);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FixedCharChar {\n");
    sb.append("    fixedLength: ").append(toIndentedString(fixedLength)).append("\n");
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

