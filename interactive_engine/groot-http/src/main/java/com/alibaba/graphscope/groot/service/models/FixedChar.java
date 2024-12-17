package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.FixedCharChar;
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
 * FixedChar
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class FixedChar implements StringTypeString {

  private FixedCharChar _char;

  public FixedChar() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public FixedChar(FixedCharChar _char) {
    this._char = _char;
  }

  public FixedChar _char(FixedCharChar _char) {
    this._char = _char;
    return this;
  }

  /**
   * Get _char
   * @return _char
  */
  @NotNull @Valid 
  @Schema(name = "char", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("char")
  public FixedCharChar getChar() {
    return _char;
  }

  public void setChar(FixedCharChar _char) {
    this._char = _char;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FixedChar fixedChar = (FixedChar) o;
    return Objects.equals(this._char, fixedChar._char);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_char);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FixedChar {\n");
    sb.append("    _char: ").append(toIndentedString(_char)).append("\n");
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

