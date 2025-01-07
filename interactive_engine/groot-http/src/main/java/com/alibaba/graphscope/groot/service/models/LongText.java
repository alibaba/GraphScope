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
 * LongText
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-01-07T12:01:45.705446+08:00[Asia/Shanghai]")
public class LongText implements StringTypeString {

  private JsonNullable<String> longText = JsonNullable.<String>undefined();

  public LongText() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public LongText(String longText) {
    this.longText = JsonNullable.of(longText);
  }

  public LongText longText(String longText) {
    this.longText = JsonNullable.of(longText);
    return this;
  }

  /**
   * Get longText
   * @return longText
  */
  @NotNull 
  @Schema(name = "long_text", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("long_text")
  public JsonNullable<String> getLongText() {
    return longText;
  }

  public void setLongText(JsonNullable<String> longText) {
    this.longText = longText;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LongText longText = (LongText) o;
    return Objects.equals(this.longText, longText.longText);
  }

  @Override
  public int hashCode() {
    return Objects.hash(longText);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class LongText {\n");
    sb.append("    longText: ").append(toIndentedString(longText)).append("\n");
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

