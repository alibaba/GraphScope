package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * CreateProcedureResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class CreateProcedureResponse {

  private String procedureId;

  public CreateProcedureResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public CreateProcedureResponse(String procedureId) {
    this.procedureId = procedureId;
  }

  public CreateProcedureResponse procedureId(String procedureId) {
    this.procedureId = procedureId;
    return this;
  }

  /**
   * Get procedureId
   * @return procedureId
  */
  @NotNull 
  @Schema(name = "procedure_id", example = "proc1", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("procedure_id")
  public String getProcedureId() {
    return procedureId;
  }

  public void setProcedureId(String procedureId) {
    this.procedureId = procedureId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateProcedureResponse createProcedureResponse = (CreateProcedureResponse) o;
    return Objects.equals(this.procedureId, createProcedureResponse.procedureId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(procedureId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateProcedureResponse {\n");
    sb.append("    procedureId: ").append(toIndentedString(procedureId)).append("\n");
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

