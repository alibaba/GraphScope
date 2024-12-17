package com.alibaba.graphscope.groot.service.models;

import java.net.URI;
import java.util.Objects;
import com.alibaba.graphscope.groot.service.models.GetGraphResponse;
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
 * ServiceStatus
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-19T17:10:03.937738+08:00[Asia/Shanghai]")
public class ServiceStatus {

  private Boolean statisticsEnabled;

  private String status;

  private GetGraphResponse graph;

  private Integer boltPort;

  private Integer hqpsPort;

  private Integer gremlinPort;

  private Integer startTime;

  public ServiceStatus statisticsEnabled(Boolean statisticsEnabled) {
    this.statisticsEnabled = statisticsEnabled;
    return this;
  }

  /**
   * Get statisticsEnabled
   * @return statisticsEnabled
  */
  
  @Schema(name = "statistics_enabled", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("statistics_enabled")
  public Boolean getStatisticsEnabled() {
    return statisticsEnabled;
  }

  public void setStatisticsEnabled(Boolean statisticsEnabled) {
    this.statisticsEnabled = statisticsEnabled;
  }

  public ServiceStatus status(String status) {
    this.status = status;
    return this;
  }

  /**
   * Get status
   * @return status
  */
  
  @Schema(name = "status", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("status")
  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public ServiceStatus graph(GetGraphResponse graph) {
    this.graph = graph;
    return this;
  }

  /**
   * Get graph
   * @return graph
  */
  @Valid 
  @Schema(name = "graph", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("graph")
  public GetGraphResponse getGraph() {
    return graph;
  }

  public void setGraph(GetGraphResponse graph) {
    this.graph = graph;
  }

  public ServiceStatus boltPort(Integer boltPort) {
    this.boltPort = boltPort;
    return this;
  }

  /**
   * Get boltPort
   * @return boltPort
  */
  
  @Schema(name = "bolt_port", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("bolt_port")
  public Integer getBoltPort() {
    return boltPort;
  }

  public void setBoltPort(Integer boltPort) {
    this.boltPort = boltPort;
  }

  public ServiceStatus hqpsPort(Integer hqpsPort) {
    this.hqpsPort = hqpsPort;
    return this;
  }

  /**
   * Get hqpsPort
   * @return hqpsPort
  */
  
  @Schema(name = "hqps_port", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("hqps_port")
  public Integer getHqpsPort() {
    return hqpsPort;
  }

  public void setHqpsPort(Integer hqpsPort) {
    this.hqpsPort = hqpsPort;
  }

  public ServiceStatus gremlinPort(Integer gremlinPort) {
    this.gremlinPort = gremlinPort;
    return this;
  }

  /**
   * Get gremlinPort
   * @return gremlinPort
  */
  
  @Schema(name = "gremlin_port", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("gremlin_port")
  public Integer getGremlinPort() {
    return gremlinPort;
  }

  public void setGremlinPort(Integer gremlinPort) {
    this.gremlinPort = gremlinPort;
  }

  public ServiceStatus startTime(Integer startTime) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceStatus serviceStatus = (ServiceStatus) o;
    return Objects.equals(this.statisticsEnabled, serviceStatus.statisticsEnabled) &&
        Objects.equals(this.status, serviceStatus.status) &&
        Objects.equals(this.graph, serviceStatus.graph) &&
        Objects.equals(this.boltPort, serviceStatus.boltPort) &&
        Objects.equals(this.hqpsPort, serviceStatus.hqpsPort) &&
        Objects.equals(this.gremlinPort, serviceStatus.gremlinPort) &&
        Objects.equals(this.startTime, serviceStatus.startTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statisticsEnabled, status, graph, boltPort, hqpsPort, gremlinPort, startTime);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ServiceStatus {\n");
    sb.append("    statisticsEnabled: ").append(toIndentedString(statisticsEnabled)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    graph: ").append(toIndentedString(graph)).append("\n");
    sb.append("    boltPort: ").append(toIndentedString(boltPort)).append("\n");
    sb.append("    hqpsPort: ").append(toIndentedString(hqpsPort)).append("\n");
    sb.append("    gremlinPort: ").append(toIndentedString(gremlinPort)).append("\n");
    sb.append("    startTime: ").append(toIndentedString(startTime)).append("\n");
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

