package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Property;
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
 * VertexData
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class VertexData {

  private String label;

  @Valid
  private List<@Valid Property> values;

  public VertexData() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public VertexData(String label) {
    this.label = label;
  }

  public VertexData label(String label) {
    this.label = label;
    return this;
  }

  /**
   * Get label
   * @return label
  */
  @NotNull 
  @Schema(name = "label", example = "person", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("label")
  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public VertexData values(List<@Valid Property> values) {
    this.values = values;
    return this;
  }

  public VertexData addValuesItem(Property valuesItem) {
    if (this.values == null) {
      this.values = new ArrayList<>();
    }
    this.values.add(valuesItem);
    return this;
  }

  /**
   * Get values
   * @return values
  */
  @Valid 
  @Schema(name = "values", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("values")
  public List<@Valid Property> getValues() {
    return values;
  }

  public void setValues(List<@Valid Property> values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexData vertexData = (VertexData) o;
    return Objects.equals(this.label, vertexData.label) &&
        Objects.equals(this.values, vertexData.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, values);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class VertexData {\n");
    sb.append("    label: ").append(toIndentedString(label)).append("\n");
    sb.append("    values: ").append(toIndentedString(values)).append("\n");
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

