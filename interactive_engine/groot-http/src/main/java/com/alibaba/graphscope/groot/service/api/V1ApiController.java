package com.alibaba.graphscope.groot.service.api;

import com.alibaba.graphscope.groot.service.impl.EdgeManagementService;
import com.alibaba.graphscope.groot.service.impl.SchemaManagementService;
import com.alibaba.graphscope.groot.service.impl.VertexManagementService;
import com.alibaba.graphscope.groot.service.models.CreateEdgeType;
import com.alibaba.graphscope.groot.service.models.CreateGraphRequest;
import com.alibaba.graphscope.groot.service.models.CreateGraphResponse;
import com.alibaba.graphscope.groot.service.models.CreateVertexType;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

import java.util.List;

import javax.validation.Valid;

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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("${openapi.graphScopeInteractiveAPIV03.base-path:/v1/graph}")
public class V1ApiController implements V1Api {

    private final VertexManagementService vertexManagementService;
    private final EdgeManagementService edgeManagementService;
    private final SchemaManagementService schemaManagementService;

    @Autowired
    public V1ApiController(VertexManagementService vertexService, EdgeManagementService edgeService,
            SchemaManagementService schemaManagementService) {
        this.vertexManagementService = vertexService;
        this.edgeManagementService = edgeService;
        this.schemaManagementService = schemaManagementService;
    }

    @Override
    @PostMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Valid VertexEdgeRequest vertexEdgeRequest) {
        try {
            long si;
            if (vertexEdgeRequest.getEdgeRequest() == null) {
                si = vertexManagementService.addVertices(vertexEdgeRequest.getVertexRequest());
            } else {
                si = vertexManagementService.addVerticesAndEdges(vertexEdgeRequest);
            }
            return ApiUtil.createSuccessResponse("Vertices and edges added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add vertices and edges");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) List<DeleteVertexRequest> deleteVertexRequest) {
        try {
            long si = vertexManagementService.deleteVertices(deleteVertexRequest);
            return ApiUtil.createSuccessResponse("Vertices deleted successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete vertices");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) List<VertexRequest> vertexRequest) {
        try {
            long si = vertexManagementService.updateVertices(vertexRequest);
            return ApiUtil.createSuccessResponse("Vertices updated successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update vertices");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) List<EdgeRequest> edgeRequest) {
        try {
            long si = edgeManagementService.addEdges(edgeRequest);
            return ApiUtil.createSuccessResponse("Edges added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add edges");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) List<DeleteEdgeRequest> deleteEdgeRequest) {
        try {
            long si = edgeManagementService.deleteEdges(deleteEdgeRequest);
            return ApiUtil.createSuccessResponse("Edges deleted successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete edges");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) List<EdgeRequest> edgeRequest) {
        try {
            long si = edgeManagementService.updateEdges(edgeRequest);
            return ApiUtil.createSuccessResponse("Edges updated successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update edges");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/schema/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createVertexType(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateVertexType createVertexType) {
        try {
            schemaManagementService.createVertexType(createVertexType);
            return ApiUtil.createSuccessResponse("Vertex type created successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create vertex type");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/schema/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteVertexTypeByName(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "type_name", required = true) String typeName) {
        try {
            schemaManagementService.deleteVertexType(typeName);
            return ApiUtil.createSuccessResponse("Vertex type deleted successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete vertex type");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/schema/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateVertexType(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateVertexType updateVertexType) {
        try {
            schemaManagementService.updateVertexType(updateVertexType);
            return ApiUtil.createSuccessResponse("Vertex type updated successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update vertex type");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/schema/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createEdgeType(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateEdgeType createEdgeType) {
        try {
            schemaManagementService.createEdgeType(createEdgeType);
            return ApiUtil.createSuccessResponse("Edge type created successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to create edge type");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/schema/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteEdgeTypeByName(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "type_name", required = true) String typeName,
            @RequestParam(value = "source_vertex_type", required = true) String sourceVertexType,
            @RequestParam(value = "destination_vertex_type", required = true) String destinationVertexType) {
        try {
            schemaManagementService.deleteEdgeType(typeName, sourceVertexType, destinationVertexType);
            return ApiUtil.createSuccessResponse("Edge type deleted successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete edge type");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/schema/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateEdgeType(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateEdgeType updateEdgeType) {
        try {
            schemaManagementService.updateEdgeType(updateEdgeType);
            return ApiUtil.createSuccessResponse("Edge type updated successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update edge type");
        }
    }

    @Override
    @PostMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CreateGraphResponse> createGraph(
            @RequestBody @Validated CreateGraphRequest createGraphRequest) {
        try {
            schemaManagementService.importSchema(createGraphRequest.getSchema());
            CreateGraphResponse response = new CreateGraphResponse();
            // a default graph id, since groot does not support multiple graphs
            response.setGraphId("0");
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @Override
    @GetMapping(value = "/{graph_id}/schema", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<GetGraphSchemaResponse> getSchema(
            @PathVariable("graph_id") String graphId) {
        try {
            GetGraphSchemaResponse response = schemaManagementService.getSchema();
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteGraph(
            @PathVariable("graph_id") String graphId) {
        try {
            schemaManagementService.dropSchema();
            return ApiUtil.createSuccessResponse("Graph schema deleted successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete graph");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/snapshot", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> flushSnapshotId(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "snapshot_id", required = true) Long snapshotId) {
        try {
            boolean res = schemaManagementService.remoteFlush(snapshotId);
            return ApiUtil.createSuccessResponse("Snapshot flushed successfully: " + res, snapshotId.longValue());
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to flush snapshot");
        }
    }

}
