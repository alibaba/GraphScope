package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
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
 * SchemaMappingLoadingConfigDataSource
 */

@JsonTypeName("SchemaMapping_loading_config_data_source")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class SchemaMappingLoadingConfigDataSource {

  /**
   * Gets or Sets scheme
   */
  public enum SchemeEnum {
    ODPS("odps"),
    
    FILE("file");

    private String value;

    SchemeEnum(String value) {
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
    public static SchemeEnum fromValue(String value) {
      for (SchemeEnum b : SchemeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private SchemeEnum scheme;

  private String location;

  public SchemaMappingLoadingConfigDataSource scheme(SchemeEnum scheme) {
    this.scheme = scheme;
    return this;
  }

  /**
   * Get scheme
   * @return scheme
  */
  
  @Schema(name = "scheme", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("scheme")
  public SchemeEnum getScheme() {
    return scheme;
  }

  public void setScheme(SchemeEnum scheme) {
    this.scheme = scheme;
  }

  public SchemaMappingLoadingConfigDataSource location(String location) {
    this.location = location;
    return this;
  }

  /**
   * Get location
   * @return location
  */
  
  @Schema(name = "location", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaMappingLoadingConfigDataSource schemaMappingLoadingConfigDataSource = (SchemaMappingLoadingConfigDataSource) o;
    return Objects.equals(this.scheme, schemaMappingLoadingConfigDataSource.scheme) &&
        Objects.equals(this.location, schemaMappingLoadingConfigDataSource.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, location);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaMappingLoadingConfigDataSource {\n");
    sb.append("    scheme: ").append(toIndentedString(scheme)).append("\n");
    sb.append("    location: ").append(toIndentedString(location)).append("\n");
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

