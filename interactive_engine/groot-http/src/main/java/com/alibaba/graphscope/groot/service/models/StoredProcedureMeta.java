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
 * StoredProcedureMeta
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class StoredProcedureMeta {

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

  public StoredProcedureMeta() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public StoredProcedureMeta(String name, TypeEnum type, String query) {
    this.name = name;
    this.type = type;
    this.query = query;
  }

  public StoredProcedureMeta name(String name) {
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

  public StoredProcedureMeta description(String description) {
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

  public StoredProcedureMeta type(TypeEnum type) {
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

  public StoredProcedureMeta query(String query) {
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

  public StoredProcedureMeta id(String id) {
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

  public StoredProcedureMeta library(String library) {
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

  public StoredProcedureMeta params(List<@Valid Parameter> params) {
    this.params = params;
    return this;
  }

  public StoredProcedureMeta addParamsItem(Parameter paramsItem) {
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

  public StoredProcedureMeta returns(List<@Valid Parameter> returns) {
    this.returns = returns;
    return this;
  }

  public StoredProcedureMeta addReturnsItem(Parameter returnsItem) {
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

  public StoredProcedureMeta enable(Boolean enable) {
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

  public StoredProcedureMeta option(Map<String, Object> option) {
    this.option = option;
    return this;
  }

  public StoredProcedureMeta putOptionItem(String key, Object optionItem) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StoredProcedureMeta storedProcedureMeta = (StoredProcedureMeta) o;
    return Objects.equals(this.name, storedProcedureMeta.name) &&
        Objects.equals(this.description, storedProcedureMeta.description) &&
        Objects.equals(this.type, storedProcedureMeta.type) &&
        Objects.equals(this.query, storedProcedureMeta.query) &&
        Objects.equals(this.id, storedProcedureMeta.id) &&
        Objects.equals(this.library, storedProcedureMeta.library) &&
        Objects.equals(this.params, storedProcedureMeta.params) &&
        Objects.equals(this.returns, storedProcedureMeta.returns) &&
        Objects.equals(this.enable, storedProcedureMeta.enable) &&
        Objects.equals(this.option, storedProcedureMeta.option);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, type, query, id, library, params, returns, enable, option);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class StoredProcedureMeta {\n");
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

