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
 * mutable_csr specific parameters
 */

@Schema(name = "SchemaMapping_loading_config_x_csr_params", description = "mutable_csr specific parameters")
@JsonTypeName("SchemaMapping_loading_config_x_csr_params")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class SchemaMappingLoadingConfigXCsrParams {

  private Integer parallelism;

  private Boolean buildCsrInMem;

  private Boolean useMmapVector;

  public SchemaMappingLoadingConfigXCsrParams parallelism(Integer parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  /**
   * Get parallelism
   * @return parallelism
  */
  
  @Schema(name = "parallelism", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("parallelism")
  public Integer getParallelism() {
    return parallelism;
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public SchemaMappingLoadingConfigXCsrParams buildCsrInMem(Boolean buildCsrInMem) {
    this.buildCsrInMem = buildCsrInMem;
    return this;
  }

  /**
   * Get buildCsrInMem
   * @return buildCsrInMem
  */
  
  @Schema(name = "build_csr_in_mem", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("build_csr_in_mem")
  public Boolean getBuildCsrInMem() {
    return buildCsrInMem;
  }

  public void setBuildCsrInMem(Boolean buildCsrInMem) {
    this.buildCsrInMem = buildCsrInMem;
  }

  public SchemaMappingLoadingConfigXCsrParams useMmapVector(Boolean useMmapVector) {
    this.useMmapVector = useMmapVector;
    return this;
  }

  /**
   * Get useMmapVector
   * @return useMmapVector
  */
  
  @Schema(name = "use_mmap_vector", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("use_mmap_vector")
  public Boolean getUseMmapVector() {
    return useMmapVector;
  }

  public void setUseMmapVector(Boolean useMmapVector) {
    this.useMmapVector = useMmapVector;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaMappingLoadingConfigXCsrParams schemaMappingLoadingConfigXCsrParams = (SchemaMappingLoadingConfigXCsrParams) o;
    return Objects.equals(this.parallelism, schemaMappingLoadingConfigXCsrParams.parallelism) &&
        Objects.equals(this.buildCsrInMem, schemaMappingLoadingConfigXCsrParams.buildCsrInMem) &&
        Objects.equals(this.useMmapVector, schemaMappingLoadingConfigXCsrParams.useMmapVector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parallelism, buildCsrInMem, useMmapVector);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaMappingLoadingConfigXCsrParams {\n");
    sb.append("    parallelism: ").append(toIndentedString(parallelism)).append("\n");
    sb.append("    buildCsrInMem: ").append(toIndentedString(buildCsrInMem)).append("\n");
    sb.append("    useMmapVector: ").append(toIndentedString(useMmapVector)).append("\n");
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

