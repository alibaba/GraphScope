package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.groot.service.models.GetProcedureResponse;
import com.alibaba.graphscope.groot.service.models.SchemaMapping;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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
 * GetGraphResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class GetGraphResponse {

  private String version;

  private String id;

  private String name;

  private String description;

  /**
   * Gets or Sets storeType
   */
  public enum StoreTypeEnum {
    MUTABLE_CSR("mutable_csr");

    private String value;

    StoreTypeEnum(String value) {
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
    public static StoreTypeEnum fromValue(String value) {
      for (StoreTypeEnum b : StoreTypeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private StoreTypeEnum storeType;

  private Integer creationTime;

  private Integer dataUpdateTime;

  @Valid
  private List<@Valid GetProcedureResponse> storedProcedures;

  private GetGraphSchemaResponse schema;

  private SchemaMapping dataImportConfig;

  public GetGraphResponse version(String version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   * @return version
  */
  
  @Schema(name = "version", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("version")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public GetGraphResponse id(String id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
  */
  
  @Schema(name = "id", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public GetGraphResponse name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
  */
  
  @Schema(name = "name", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public GetGraphResponse description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Get description
   * @return description
  */
  
  @Schema(name = "description", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public GetGraphResponse storeType(StoreTypeEnum storeType) {
    this.storeType = storeType;
    return this;
  }

  /**
   * Get storeType
   * @return storeType
  */
  
  @Schema(name = "store_type", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("store_type")
  public StoreTypeEnum getStoreType() {
    return storeType;
  }

  public void setStoreType(StoreTypeEnum storeType) {
    this.storeType = storeType;
  }

  public GetGraphResponse creationTime(Integer creationTime) {
    this.creationTime = creationTime;
    return this;
  }

  /**
   * Get creationTime
   * @return creationTime
  */
  
  @Schema(name = "creation_time", example = "11223444", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("creation_time")
  public Integer getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Integer creationTime) {
    this.creationTime = creationTime;
  }

  public GetGraphResponse dataUpdateTime(Integer dataUpdateTime) {
    this.dataUpdateTime = dataUpdateTime;
    return this;
  }

  /**
   * Get dataUpdateTime
   * @return dataUpdateTime
  */
  
  @Schema(name = "data_update_time", example = "11123445", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("data_update_time")
  public Integer getDataUpdateTime() {
    return dataUpdateTime;
  }

  public void setDataUpdateTime(Integer dataUpdateTime) {
    this.dataUpdateTime = dataUpdateTime;
  }

  public GetGraphResponse storedProcedures(List<@Valid GetProcedureResponse> storedProcedures) {
    this.storedProcedures = storedProcedures;
    return this;
  }

  public GetGraphResponse addStoredProceduresItem(GetProcedureResponse storedProceduresItem) {
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
  public List<@Valid GetProcedureResponse> getStoredProcedures() {
    return storedProcedures;
  }

  public void setStoredProcedures(List<@Valid GetProcedureResponse> storedProcedures) {
    this.storedProcedures = storedProcedures;
  }

  public GetGraphResponse schema(GetGraphSchemaResponse schema) {
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
  public GetGraphSchemaResponse getSchema() {
    return schema;
  }

  public void setSchema(GetGraphSchemaResponse schema) {
    this.schema = schema;
  }

  public GetGraphResponse dataImportConfig(SchemaMapping dataImportConfig) {
    this.dataImportConfig = dataImportConfig;
    return this;
  }

  /**
   * Get dataImportConfig
   * @return dataImportConfig
  */
  @Valid 
  @Schema(name = "data_import_config", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("data_import_config")
  public SchemaMapping getDataImportConfig() {
    return dataImportConfig;
  }

  public void setDataImportConfig(SchemaMapping dataImportConfig) {
    this.dataImportConfig = dataImportConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetGraphResponse getGraphResponse = (GetGraphResponse) o;
    return Objects.equals(this.version, getGraphResponse.version) &&
        Objects.equals(this.id, getGraphResponse.id) &&
        Objects.equals(this.name, getGraphResponse.name) &&
        Objects.equals(this.description, getGraphResponse.description) &&
        Objects.equals(this.storeType, getGraphResponse.storeType) &&
        Objects.equals(this.creationTime, getGraphResponse.creationTime) &&
        Objects.equals(this.dataUpdateTime, getGraphResponse.dataUpdateTime) &&
        Objects.equals(this.storedProcedures, getGraphResponse.storedProcedures) &&
        Objects.equals(this.schema, getGraphResponse.schema) &&
        Objects.equals(this.dataImportConfig, getGraphResponse.dataImportConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, id, name, description, storeType, creationTime, dataUpdateTime, storedProcedures, schema, dataImportConfig);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetGraphResponse {\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    storeType: ").append(toIndentedString(storeType)).append("\n");
    sb.append("    creationTime: ").append(toIndentedString(creationTime)).append("\n");
    sb.append("    dataUpdateTime: ").append(toIndentedString(dataUpdateTime)).append("\n");
    sb.append("    storedProcedures: ").append(toIndentedString(storedProcedures)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    dataImportConfig: ").append(toIndentedString(dataImportConfig)).append("\n");
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

