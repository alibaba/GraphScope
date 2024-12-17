package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.CreateGraphSchemaRequest;
import com.alibaba.graphscope.groot.service.models.CreateProcedureRequest;
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
 * CreateGraphRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class CreateGraphRequest {

  private String name;

  private String description;

  @Valid
  private List<@Valid CreateProcedureRequest> storedProcedures;

  private CreateGraphSchemaRequest schema;

  public CreateGraphRequest name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
  */
  
  @Schema(name = "name", example = "modern_graph", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CreateGraphRequest description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * @return description
  */
  
  @Schema(name = "description", example = "A default description", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public CreateGraphRequest storedProcedures(List<@Valid CreateProcedureRequest> storedProcedures) {
    this.storedProcedures = storedProcedures;
    return this;
  }

  public CreateGraphRequest addStoredProceduresItem(CreateProcedureRequest storedProceduresItem) {
    if (this.storedProcedures == null) {
      this.storedProcedures = new ArrayList<>();
    }
    this.storedProcedures.add(storedProceduresItem);
    return this;
  }

  /**
   * Get storedProcedures
   * @return storedProcedures
  */
  @Valid 
  @Schema(name = "stored_procedures", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("stored_procedures")
  public List<@Valid CreateProcedureRequest> getStoredProcedures() {
    return storedProcedures;
  }

  public void setStoredProcedures(List<@Valid CreateProcedureRequest> storedProcedures) {
    this.storedProcedures = storedProcedures;
  }

  public CreateGraphRequest schema(CreateGraphSchemaRequest schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Get schema
   * @return schema
  */
  @Valid 
  @Schema(name = "schema", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("schema")
  public CreateGraphSchemaRequest getSchema() {
    return schema;
  }

  public void setSchema(CreateGraphSchemaRequest schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateGraphRequest createGraphRequest = (CreateGraphRequest) o;
    return Objects.equals(this.name, createGraphRequest.name) &&
        Objects.equals(this.description, createGraphRequest.description) &&
        Objects.equals(this.storedProcedures, createGraphRequest.storedProcedures) &&
        Objects.equals(this.schema, createGraphRequest.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, storedProcedures, schema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateGraphRequest {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    storedProcedures: ").append(toIndentedString(storedProcedures)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
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

