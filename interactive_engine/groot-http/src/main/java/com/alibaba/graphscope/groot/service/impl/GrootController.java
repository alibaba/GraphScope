package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.service.models.APIResponseWithCode;
import com.alibaba.graphscope.groot.service.models.CreateGraphRequest;
import com.alibaba.graphscope.groot.service.models.CreateGraphResponse;
import com.alibaba.graphscope.groot.service.models.CreateProcedureRequest;
import com.alibaba.graphscope.groot.service.models.CreateProcedureResponse;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.EdgeData;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.GetGraphResponse;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.groot.service.models.GetGraphStatisticsResponse;
import com.alibaba.graphscope.groot.service.models.GetProcedureResponse;
import com.alibaba.graphscope.groot.service.models.JobResponse;
import com.alibaba.graphscope.groot.service.models.JobStatus;
import com.alibaba.graphscope.groot.service.models.Property;
import com.alibaba.graphscope.groot.service.models.SchemaMapping;
import com.alibaba.graphscope.groot.service.models.ServiceStatus;
import com.alibaba.graphscope.groot.service.models.StartServiceRequest;
import com.alibaba.graphscope.groot.service.models.StopServiceRequest;
import com.alibaba.graphscope.groot.service.models.UpdateProcedureRequest;
import com.alibaba.graphscope.groot.service.models.UploadFileResponse;
import com.alibaba.graphscope.groot.service.models.VertexData;
import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;

import com.alibaba.graphscope.groot.service.api.ApiUtil;
import com.alibaba.graphscope.groot.service.api.V1Api;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.checkerframework.checker.units.qual.g;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;

import org.apache.tinkerpop.gremlin.driver.Client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

@RestController
@RequestMapping("${openapi.graphScopeInteractiveAPIV03.base-path:/v1/graph}")
public class GrootController implements V1Api {

    private final VertexManagementService vertexManagementService;
    private final EdgeManagementService edgeManagementService;

    @Autowired
    public GrootController(VertexManagementService vertexService, EdgeManagementService edgeService) {
        this.vertexManagementService = vertexService;
        this.edgeManagementService = edgeService;
    }

    @Override
    @PostMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated List<VertexRequest> vertexRequests) {
        try {
            if (vertexRequests.isEmpty()) {
                return ResponseEntity.status(400).body("{\"error\": \"Vertex request must not be empty\"}");
            }
            long si = vertexManagementService.addVertices(vertexRequests);
            return ResponseEntity.status(200)
                    .body("{\"message\": \"Vertex added successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("{\"error\": \"Failed to add vertex\"}");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/vertex", produces =MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteVertex(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "label", required = true) String label,
            @RequestBody(required = true) List<Property> primaryKeyValues) {
        try {
            long si = vertexManagementService.deleteVertex(label, primaryKeyValues);
            return ResponseEntity.status(200)
                    .body("{\"message\": \"Vertex deleted successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("{\"error\": \"Failed to delete vertex\"}");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) VertexRequest vertexRequest) {
        try {
            if (vertexRequest == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"error\": \"Request body must not be null\"}");
            }
            long si = vertexManagementService.updateVertex(vertexRequest);
            return ResponseEntity.status(HttpStatus.OK).body("{\"message\": \"Vertex updated successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("{\"error\": \"Failed to update vertex\"}");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated List<EdgeRequest> edgeRequests) {
        try {
            if (edgeRequests.isEmpty()) {
                return ResponseEntity.status(400).body("{\"error\": \"Edge request must not be empty\"}");
            }
            System.out.println("edgeRequests: " + edgeRequests);
            long si = edgeManagementService.addEdges(edgeRequests);
            return ResponseEntity.status(200)
                    .body("{\"message\": \"Edge added successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("{\"error\": \"Failed to add edge\"}");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteEdge(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "edge_label", required = true) String label,
            @RequestParam(value = "src_label", required = true) String srcLabel,
            @RequestParam(value = "dst_label", required = true) String dstLabel,
            @RequestBody(required = true) DeleteEdgeRequest deleteEdgeRequest) {
        try {
            long si = edgeManagementService.deleteEdge(label, srcLabel, dstLabel, deleteEdgeRequest.getSrcPrimaryKeyValues(), deleteEdgeRequest.getDstPrimaryKeyValues());
            return ResponseEntity.status(200)
                    .body("{\"message\": \"Edge deleted successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("{\"error\": \"Failed to delete edge\"}");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) EdgeRequest edgeRequest) {
        try {
            if (edgeRequest == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"error\": \"Request body must not be null\"}");
            }
            long si = edgeManagementService.updateEdge(edgeRequest);
            return ResponseEntity.status(HttpStatus.OK).body("{\"message\": \"Edge updated successfully\", \"snapshot id\": " + si + "}");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("{\"error\": \"Failed to update edge\"}");
        }
    }


}
