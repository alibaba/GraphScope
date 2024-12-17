package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.HashMap;
import java.util.Map;
import org.openapitools.jackson.nullable.JsonNullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * UploadFileResponse
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class UploadFileResponse {

  private String filePath;

  @Valid
  private Map<String, Object> metadata = new HashMap<>();

  public UploadFileResponse() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public UploadFileResponse(String filePath) {
    this.filePath = filePath;
  }

  public UploadFileResponse filePath(String filePath) {
    this.filePath = filePath;
    return this;
  }

  /**
   * Get filePath
   * @return filePath
  */
  @NotNull 
  @Schema(name = "file_path", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("file_path")
  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public UploadFileResponse metadata(Map<String, Object> metadata) {
    this.metadata = metadata;
    return this;
  }

  public UploadFileResponse putMetadataItem(String key, Object metadataItem) {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.put(key, metadataItem);
    return this;
  }

  /**
   * Get metadata
   * @return metadata
  */
  
  @Schema(name = "metadata", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("metadata")
  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UploadFileResponse uploadFileResponse = (UploadFileResponse) o;
    return Objects.equals(this.filePath, uploadFileResponse.filePath) &&
        Objects.equals(this.metadata, uploadFileResponse.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, metadata);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UploadFileResponse {\n");
    sb.append("    filePath: ").append(toIndentedString(filePath)).append("\n");
    sb.append("    metadata: ").append(toIndentedString(metadata)).append("\n");
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

