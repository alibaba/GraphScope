package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.TypedValue;
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
 * QueryRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class QueryRequest {

  private String queryName;

  @Valid
  private List<@Valid TypedValue> arguments;

  public QueryRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public QueryRequest(String queryName) {
    this.queryName = queryName;
  }

  public QueryRequest queryName(String queryName) {
    this.queryName = queryName;
    return this;
  }

  /**
   * Get queryName
   * @return queryName
  */
  @NotNull 
  @Schema(name = "query_name", example = "ic1", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("query_name")
  public String getQueryName() {
    return queryName;
  }

  public void setQueryName(String queryName) {
    this.queryName = queryName;
  }

  public QueryRequest arguments(List<@Valid TypedValue> arguments) {
    this.arguments = arguments;
    return this;
  }

  public QueryRequest addArgumentsItem(TypedValue argumentsItem) {
    if (this.arguments == null) {
      this.arguments = new ArrayList<>();
    }
    this.arguments.add(argumentsItem);
    return this;
  }

  /**
   * Get arguments
   * @return arguments
  */
  @Valid 
  @Schema(name = "arguments", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("arguments")
  public List<@Valid TypedValue> getArguments() {
    return arguments;
  }

  public void setArguments(List<@Valid TypedValue> arguments) {
    this.arguments = arguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryRequest queryRequest = (QueryRequest) o;
    return Objects.equals(this.queryName, queryRequest.queryName) &&
        Objects.equals(this.arguments, queryRequest.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryName, arguments);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class QueryRequest {\n");
    sb.append("    queryName: ").append(toIndentedString(queryName)).append("\n");
    sb.append("    arguments: ").append(toIndentedString(arguments)).append("\n");
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

