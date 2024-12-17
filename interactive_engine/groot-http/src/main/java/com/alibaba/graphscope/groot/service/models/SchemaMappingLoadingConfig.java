package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.SchemaMappingLoadingConfigDataSource;
import com.alibaba.graphscope.groot.service.models.SchemaMappingLoadingConfigFormat;
import com.alibaba.graphscope.groot.service.models.SchemaMappingLoadingConfigXCsrParams;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * SchemaMappingLoadingConfig
 */

@JsonTypeName("SchemaMapping_loading_config")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class SchemaMappingLoadingConfig {

  private SchemaMappingLoadingConfigXCsrParams xCsrParams;

  private SchemaMappingLoadingConfigDataSource dataSource;

  /**
   * Gets or Sets importOption
   */
  public enum ImportOptionEnum {
    INIT("init"),
    
    OVERWRITE("overwrite");

    private String value;

    ImportOptionEnum(String value) {
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
    public static ImportOptionEnum fromValue(String value) {
      for (ImportOptionEnum b : ImportOptionEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private ImportOptionEnum importOption;

  private SchemaMappingLoadingConfigFormat format;

  public SchemaMappingLoadingConfig xCsrParams(SchemaMappingLoadingConfigXCsrParams xCsrParams) {
    this.xCsrParams = xCsrParams;
    return this;
  }

  /**
   * Get xCsrParams
   * @return xCsrParams
  */
  @Valid 
  @Schema(name = "x_csr_params", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("x_csr_params")
  public SchemaMappingLoadingConfigXCsrParams getxCsrParams() {
    return xCsrParams;
  }

  public void setxCsrParams(SchemaMappingLoadingConfigXCsrParams xCsrParams) {
    this.xCsrParams = xCsrParams;
  }

  public SchemaMappingLoadingConfig dataSource(SchemaMappingLoadingConfigDataSource dataSource) {
    this.dataSource = dataSource;
    return this;
  }

  /**
   * Get dataSource
   * @return dataSource
  */
  @Valid 
  @Schema(name = "data_source", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("data_source")
  public SchemaMappingLoadingConfigDataSource getDataSource() {
    return dataSource;
  }

  public void setDataSource(SchemaMappingLoadingConfigDataSource dataSource) {
    this.dataSource = dataSource;
  }

  public SchemaMappingLoadingConfig importOption(ImportOptionEnum importOption) {
    this.importOption = importOption;
    return this;
  }

  /**
   * Get importOption
   * @return importOption
  */
  
  @Schema(name = "import_option", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("import_option")
  public ImportOptionEnum getImportOption() {
    return importOption;
  }

  public void setImportOption(ImportOptionEnum importOption) {
    this.importOption = importOption;
  }

  public SchemaMappingLoadingConfig format(SchemaMappingLoadingConfigFormat format) {
    this.format = format;
    return this;
  }

  /**
   * Get format
   * @return format
  */
  @Valid 
  @Schema(name = "format", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("format")
  public SchemaMappingLoadingConfigFormat getFormat() {
    return format;
  }

  public void setFormat(SchemaMappingLoadingConfigFormat format) {
    this.format = format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaMappingLoadingConfig schemaMappingLoadingConfig = (SchemaMappingLoadingConfig) o;
    return Objects.equals(this.xCsrParams, schemaMappingLoadingConfig.xCsrParams) &&
        Objects.equals(this.dataSource, schemaMappingLoadingConfig.dataSource) &&
        Objects.equals(this.importOption, schemaMappingLoadingConfig.importOption) &&
        Objects.equals(this.format, schemaMappingLoadingConfig.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(xCsrParams, dataSource, importOption, format);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaMappingLoadingConfig {\n");
    sb.append("    xCsrParams: ").append(toIndentedString(xCsrParams)).append("\n");
    sb.append("    dataSource: ").append(toIndentedString(dataSource)).append("\n");
    sb.append("    importOption: ").append(toIndentedString(importOption)).append("\n");
    sb.append("    format: ").append(toIndentedString(format)).append("\n");
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

