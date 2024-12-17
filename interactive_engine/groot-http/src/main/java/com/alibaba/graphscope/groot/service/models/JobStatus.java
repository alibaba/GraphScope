package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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
 * JobStatus
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class JobStatus {

  private String id;

  private String type;

  /**
   * Gets or Sets status
   */
  public enum StatusEnum {
    RUNNING("RUNNING"),
    
    SUCCESS("SUCCESS"),
    
    FAILED("FAILED"),
    
    CANCELLED("CANCELLED"),
    
    WAITING("WAITING");

    private String value;

    StatusEnum(String value) {
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
    public static StatusEnum fromValue(String value) {
      for (StatusEnum b : StatusEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private StatusEnum status;

  private Integer startTime;

  private Integer endTime;

  private String log;

  @Valid
  private Map<String, Object> detail = new HashMap<>();

  public JobStatus id(String id) {
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

  public JobStatus type(String type) {
    this.type = type;
    return this;
  }

  /**
   * Get type
   * @return type
  */
  
  @Schema(name = "type", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("type")
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public JobStatus status(StatusEnum status) {
    this.status = status;
    return this;
  }

  /**
   * Get status
   * @return status
  */
  
  @Schema(name = "status", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("status")
  public StatusEnum getStatus() {
    return status;
  }

  public void setStatus(StatusEnum status) {
    this.status = status;
  }

  public JobStatus startTime(Integer startTime) {
    this.startTime = startTime;
    return this;
  }

  /**
   * Get startTime
   * @return startTime
  */
  
  @Schema(name = "start_time", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("start_time")
  public Integer getStartTime() {
    return startTime;
  }

  public void setStartTime(Integer startTime) {
    this.startTime = startTime;
  }

  public JobStatus endTime(Integer endTime) {
    this.endTime = endTime;
    return this;
  }

  /**
   * Get endTime
   * @return endTime
  */
  
  @Schema(name = "end_time", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("end_time")
  public Integer getEndTime() {
    return endTime;
  }

  public void setEndTime(Integer endTime) {
    this.endTime = endTime;
  }

  public JobStatus log(String log) {
    this.log = log;
    return this;
  }

  /**
   * URL or log string
   * @return log
  */
  
  @Schema(name = "log", description = "URL or log string", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("log")
  public String getLog() {
    return log;
  }

  public void setLog(String log) {
    this.log = log;
  }

  public JobStatus detail(Map<String, Object> detail) {
    this.detail = detail;
    return this;
  }

  public JobStatus putDetailItem(String key, Object detailItem) {
    if (this.detail == null) {
      this.detail = new HashMap<>();
    }
    this.detail.put(key, detailItem);
    return this;
  }

  /**
   * Get detail
   * @return detail
  */
  
  @Schema(name = "detail", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("detail")
  public Map<String, Object> getDetail() {
    return detail;
  }

  public void setDetail(Map<String, Object> detail) {
    this.detail = detail;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobStatus jobStatus = (JobStatus) o;
    return Objects.equals(this.id, jobStatus.id) &&
        Objects.equals(this.type, jobStatus.type) &&
        Objects.equals(this.status, jobStatus.status) &&
        Objects.equals(this.startTime, jobStatus.startTime) &&
        Objects.equals(this.endTime, jobStatus.endTime) &&
        Objects.equals(this.log, jobStatus.log) &&
        Objects.equals(this.detail, jobStatus.detail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, status, startTime, endTime, log, detail);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class JobStatus {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    startTime: ").append(toIndentedString(startTime)).append("\n");
    sb.append("    endTime: ").append(toIndentedString(endTime)).append("\n");
    sb.append("    log: ").append(toIndentedString(log)).append("\n");
    sb.append("    detail: ").append(toIndentedString(detail)).append("\n");
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

