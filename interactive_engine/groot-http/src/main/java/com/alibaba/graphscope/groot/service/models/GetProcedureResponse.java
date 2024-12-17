package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.Parameter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * GetProcedureResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetProcedureResponse {

  private String name;

  private String description;

  /**
   * Gets or Sets type
   */
  public enum TypeEnum {
    CPP("cpp"),
    
    CYPHER("cypher");

    private String value;

    TypeEnum(String value) {
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
    public static TypeEnum fromValue(String value) {
      for (TypeEnum b : TypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private TypeEnum type;

  private String query;

  private String id;

  private String library;

  @Valid
  private List<@Valid Parameter> params;

  @Valid
  private List<@Valid Parameter> returns;

  private Boolean enable;

  @Valid
  private Map<String, Object> option = new HashMap<>();

  private String boundGraph;

  private Boolean runnable;

  private Integer creationTime;

  private Integer updateTime;

  public GetProcedureResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public GetProcedureResponse(String name, TypeEnum type, String query) {
    this.name = name;
    this.type = type;
    this.query = query;
  }

  public GetProcedureResponse name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
  */
  @NotNull 
  @Schema(name = "name", example = "query1", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public GetProcedureResponse description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * @return description
  */
  
  @Schema(name = "description", example = "A sample stored procedure", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public GetProcedureResponse type(TypeEnum type) {
    this.type = type;
    return this;
  }

  /**
   * Get type
   * @return type
  */
  @NotNull 
  @Schema(name = "type", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("type")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  public GetProcedureResponse query(String query) {
    this.query = query;
    return this;
  }

  /**
   * Get query
   * @return query
  */
  @NotNull 
  @Schema(name = "query", example = "MATCH(a) return COUNT(a);", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("query")
  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public GetProcedureResponse id(String id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
  */
  
  @Schema(name = "id", example = "The unique identifier of procedure, currently is same with name.", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public GetProcedureResponse library(String library) {
    this.library = library;
    return this;
  }

  /**
   * Get library
   * @return library
  */
  
  @Schema(name = "library", example = "/path/to/library", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("library")
  public String getLibrary() {
    return library;
  }

  public void setLibrary(String library) {
    this.library = library;
  }

  public GetProcedureResponse params(List<@Valid Parameter> params) {
    this.params = params;
    return this;
  }

  public GetProcedureResponse addParamsItem(Parameter paramsItem) {
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
    this.params.add(paramsItem);
    return this;
  }

  /**
   * Get params
   * @return params
  */
  @Valid 
  @Schema(name = "params", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("params")
  public List<@Valid Parameter> getParams() {
    return params;
  }

  public void setParams(List<@Valid Parameter> params) {
    this.params = params;
  }

  public GetProcedureResponse returns(List<@Valid Parameter> returns) {
    this.returns = returns;
    return this;
  }

  public GetProcedureResponse addReturnsItem(Parameter returnsItem) {
    if (this.returns == null) {
      this.returns = new ArrayList<>();
    }
    this.returns.add(returnsItem);
    return this;
  }

  /**
   * Get returns
   * @return returns
  */
  @Valid 
  @Schema(name = "returns", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("returns")
  public List<@Valid Parameter> getReturns() {
    return returns;
  }

  public void setReturns(List<@Valid Parameter> returns) {
    this.returns = returns;
  }

  public GetProcedureResponse enable(Boolean enable) {
    this.enable = enable;
    return this;
  }

  /**
   * Get enable
   * @return enable
  */
  
  @Schema(name = "enable", example = "true", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("enable")
  public Boolean getEnable() {
    return enable;
  }

  public void setEnable(Boolean enable) {
    this.enable = enable;
  }

  public GetProcedureResponse option(Map<String, Object> option) {
    this.option = option;
    return this;
  }

  public GetProcedureResponse putOptionItem(String key, Object optionItem) {
    if (this.option == null) {
      this.option = new HashMap<>();
    }
    this.option.put(key, optionItem);
    return this;
  }

  /**
   * Get option
   * @return option
  */
  
  @Schema(name = "option", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("option")
  public Map<String, Object> getOption() {
    return option;
  }

  public void setOption(Map<String, Object> option) {
    this.option = option;
  }

  public GetProcedureResponse boundGraph(String boundGraph) {
    this.boundGraph = boundGraph;
    return this;
  }

  /**
   * Get boundGraph
   * @return boundGraph
  */
  
  @Schema(name = "bound_graph", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("bound_graph")
  public String getBoundGraph() {
    return boundGraph;
  }

  public void setBoundGraph(String boundGraph) {
    this.boundGraph = boundGraph;
  }

  public GetProcedureResponse runnable(Boolean runnable) {
    this.runnable = runnable;
    return this;
  }

  /**
   * Get runnable
   * @return runnable
  */
  
  @Schema(name = "runnable", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("runnable")
  public Boolean getRunnable() {
    return runnable;
  }

  public void setRunnable(Boolean runnable) {
    this.runnable = runnable;
  }

  public GetProcedureResponse creationTime(Integer creationTime) {
    this.creationTime = creationTime;
    return this;
  }

  /**
   * Get creationTime
   * @return creationTime
  */
  
  @Schema(name = "creation_time", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("creation_time")
  public Integer getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Integer creationTime) {
    this.creationTime = creationTime;
  }

  public GetProcedureResponse updateTime(Integer updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  /**
   * Get updateTime
   * @return updateTime
  */
  
  @Schema(name = "update_time", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("update_time")
  public Integer getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Integer updateTime) {
    this.updateTime = updateTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetProcedureResponse getProcedureResponse = (GetProcedureResponse) o;
    return Objects.equals(this.name, getProcedureResponse.name) &&
        Objects.equals(this.description, getProcedureResponse.description) &&
        Objects.equals(this.type, getProcedureResponse.type) &&
        Objects.equals(this.query, getProcedureResponse.query) &&
        Objects.equals(this.id, getProcedureResponse.id) &&
        Objects.equals(this.library, getProcedureResponse.library) &&
        Objects.equals(this.params, getProcedureResponse.params) &&
        Objects.equals(this.returns, getProcedureResponse.returns) &&
        Objects.equals(this.enable, getProcedureResponse.enable) &&
        Objects.equals(this.option, getProcedureResponse.option) &&
        Objects.equals(this.boundGraph, getProcedureResponse.boundGraph) &&
        Objects.equals(this.runnable, getProcedureResponse.runnable) &&
        Objects.equals(this.creationTime, getProcedureResponse.creationTime) &&
        Objects.equals(this.updateTime, getProcedureResponse.updateTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, type, query, id, library, params, returns, enable, option, boundGraph, runnable, creationTime, updateTime);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetProcedureResponse {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    query: ").append(toIndentedString(query)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    library: ").append(toIndentedString(library)).append("\n");
    sb.append("    params: ").append(toIndentedString(params)).append("\n");
    sb.append("    returns: ").append(toIndentedString(returns)).append("\n");
    sb.append("    enable: ").append(toIndentedString(enable)).append("\n");
    sb.append("    option: ").append(toIndentedString(option)).append("\n");
    sb.append("    boundGraph: ").append(toIndentedString(boundGraph)).append("\n");
    sb.append("    runnable: ").append(toIndentedString(runnable)).append("\n");
    sb.append("    creationTime: ").append(toIndentedString(creationTime)).append("\n");
    sb.append("    updateTime: ").append(toIndentedString(updateTime)).append("\n");
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

