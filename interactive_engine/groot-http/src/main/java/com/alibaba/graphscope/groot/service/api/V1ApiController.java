package com.alibaba.graphscope.groot.service.api;

import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.service.impl.EdgeManagementService;
import com.alibaba.graphscope.groot.service.impl.SchemaManagementService;
import com.alibaba.graphscope.groot.service.impl.VertexManagementService;
import com.alibaba.graphscope.groot.service.models.CreateEdgeType;
import com.alibaba.graphscope.groot.service.models.CreateGraphSchemaRequest;
import com.alibaba.graphscope.groot.service.models.CreateVertexType;
import com.alibaba.graphscope.groot.service.models.DeleteEdgeRequest;
import com.alibaba.graphscope.groot.service.models.DeleteVertexRequest;
import com.alibaba.graphscope.groot.service.models.EdgeRequest;
import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
import com.alibaba.graphscope.groot.service.models.VertexRequest;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;

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
import org.springframework.web.bind.annotation.RequestMethod;
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
            @RequestBody @Validated VertexRequest vertexRequest) {
        try {
            long si = vertexManagementService.addVertex(vertexRequest);
            return ApiUtil.createSuccessResponse("Vertex added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add vertex");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) DeleteVertexRequest deleteVertexRequest) {
        try {
            long si = vertexManagementService.deleteVertex(deleteVertexRequest);
            return ApiUtil.createSuccessResponse("Vertex deleted successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete vertex");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateVertex(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) VertexRequest vertexRequest) {
        try {
            if (vertexRequest == null) {
                return ApiUtil.createErrorResponse(HttpStatus.BAD_REQUEST, "Request body must not be null");
            }
            long si = vertexManagementService.updateVertex(vertexRequest);
            return ApiUtil.createSuccessResponse("Vertex updated successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update vertex");
        }
    }

   
    @Override
    @PostMapping(value = "/{graph_id}/vertex/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addVertices(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) List<VertexRequest> vertexRequest) {
        try {
            long si = vertexManagementService.addVertices(vertexRequest);
            return ApiUtil.createSuccessResponse("Vertices added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add vertices");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/vertex/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteVertices(
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
    @PutMapping(value = "/{graph_id}/vertex/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateVertices(
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
            @RequestBody @Validated EdgeRequest edgeRequest) {
        try {
            long si = edgeManagementService.addEdge(edgeRequest);
            return ApiUtil.createSuccessResponse("Edge added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add edge");
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = true) DeleteEdgeRequest deleteEdgeRequest) {
        try {
            long si = edgeManagementService.deleteEdge(deleteEdgeRequest);
            return ApiUtil.createSuccessResponse("Edge deleted successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete edge");
        }
    }

    @Override
    @PutMapping(value = "/{graph_id}/edge", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateEdge(
            @PathVariable("graph_id") String graphId,
            @RequestBody(required = false) EdgeRequest edgeRequest) {
        try {
            if (edgeRequest == null) {
                return ApiUtil.createErrorResponse(HttpStatus.BAD_REQUEST, "Request body must not be null");
            }
            long si = edgeManagementService.updateEdge(edgeRequest);
            return ApiUtil.createSuccessResponse("Edge updated successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update edge");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/edge/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addEdges(
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
    @DeleteMapping(value = "/{graph_id}/edge/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteEdges(
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
    @PutMapping(value = "/{graph_id}/edge/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateEdges(
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
    @PostMapping(value = "/{graph_id}/vertex-edge/bulk", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> addVerticesEdges(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Valid VertexEdgeRequest vertexEdgeRequest) {
        try {
            long si = vertexManagementService.addVerticesAndEdges(vertexEdgeRequest);
            return ApiUtil.createSuccessResponse("Vertices and edges added successfully", si);
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to add vertices and edges");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/schema/vertex", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createVertexType(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateVertexType createVertexType) {
        try {
            schemaManagementService.createVertexType(graphId, createVertexType);
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
            schemaManagementService.deleteVertexType(graphId, typeName);
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
            schemaManagementService.updateVertexType(graphId, updateVertexType);
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
            schemaManagementService.createEdgeType(graphId, createEdgeType);
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
            schemaManagementService.deleteEdgeType(graphId, typeName, sourceVertexType, destinationVertexType);
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
            schemaManagementService.updateEdgeType(graphId, updateEdgeType);
            return ApiUtil.createSuccessResponse("Edge type updated successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to update edge type");
        }
    }

    @Override
    @PostMapping(value = "/{graph_id}/schema", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> importSchema(
            @PathVariable("graph_id") String graphId,
            @RequestBody @Validated CreateGraphSchemaRequest createGraphSchemaRequest) {
        try {
            schemaManagementService.importSchema(graphId, createGraphSchemaRequest);
            return ApiUtil.createSuccessResponse("Schema imported successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to import schema");
        }
    }

    @Override
    @GetMapping(value = "/{graph_id}/schema", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<GetGraphSchemaResponse> getSchema(
            @PathVariable("graph_id") String graphId) {
        try {
            GetGraphSchemaResponse response = schemaManagementService.getSchema(graphId);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @Override
    @DeleteMapping(value = "/{graph_id}/schema", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteSchema(
            @PathVariable("graph_id") String graphId) {
        try {
            schemaManagementService.dropSchema(graphId);
            return ApiUtil.createSuccessResponse("Schema deleted successfully");
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to delete schema");
        }
    }


    @Override
    @PostMapping(value = "/{graph_id}/flush", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> remoteFlush(
            @PathVariable("graph_id") String graphId,
            @RequestParam(value = "snapshot_id", required = true) Long snapshotId) {
        try {
            boolean res = vertexManagementService.remoteFlush(snapshotId);
            return ApiUtil.createSuccessResponse("Snapshot flushed successfully: " + res, snapshotId.longValue());
        } catch (Exception e) {
            e.printStackTrace();
            return ApiUtil.createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to flush snapshot");
        }
    }

}
