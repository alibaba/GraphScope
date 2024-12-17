package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.VarCharVarChar;
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
 * VarChar
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VarChar implements StringTypeString {

  private VarCharVarChar varChar;

  public VarChar() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public VarChar(VarCharVarChar varChar) {
    this.varChar = varChar;
  }

  public VarChar varChar(VarCharVarChar varChar) {
    this.varChar = varChar;
    return this;
  }

  /**
   * Get varChar
   * @return varChar
  */
  @NotNull @Valid 
  @Schema(name = "var_char", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("var_char")
  public VarCharVarChar getVarChar() {
    return varChar;
  }

  public void setVarChar(VarCharVarChar varChar) {
    this.varChar = varChar;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VarChar varChar = (VarChar) o;
    return Objects.equals(this.varChar, varChar.varChar);
  }

  @Override
  public int hashCode() {
    return Objects.hash(varChar);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VarChar {\n");
    sb.append("    varChar: ").append(toIndentedString(varChar)).append("\n");
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

