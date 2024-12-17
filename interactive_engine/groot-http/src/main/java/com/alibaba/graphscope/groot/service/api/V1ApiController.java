// package com.alibaba.graphscope.groot.service.api;

// import com.alibaba.graphscope.groot.service.models.APIResponseWithCode;
// import com.alibaba.graphscope.groot.service.models.CreateGraphRequest;
// import com.alibaba.graphscope.groot.service.models.CreateGraphResponse;
// import com.alibaba.graphscope.groot.service.models.CreateProcedureRequest;
// import com.alibaba.graphscope.groot.service.models.CreateProcedureResponse;
// import com.alibaba.graphscope.groot.service.models.EdgeData;
// import com.alibaba.graphscope.groot.service.models.EdgeRequest;
// import com.alibaba.graphscope.groot.service.models.GetGraphResponse;
// import com.alibaba.graphscope.groot.service.models.GetGraphSchemaResponse;
// import com.alibaba.graphscope.groot.service.models.GetGraphStatisticsResponse;
// import com.alibaba.graphscope.groot.service.models.GetProcedureResponse;
// import com.alibaba.graphscope.groot.service.models.JobResponse;
// import com.alibaba.graphscope.groot.service.models.JobStatus;
// import com.alibaba.graphscope.groot.service.models.SchemaMapping;
// import com.alibaba.graphscope.groot.service.models.ServiceStatus;
// import com.alibaba.graphscope.groot.service.models.StartServiceRequest;
// import com.alibaba.graphscope.groot.service.models.StopServiceRequest;
// import com.alibaba.graphscope.groot.service.models.UpdateProcedureRequest;
// import com.alibaba.graphscope.groot.service.models.UploadFileResponse;
// import com.alibaba.graphscope.groot.service.models.VertexData;
// import com.alibaba.graphscope.groot.service.models.VertexEdgeRequest;
// import com.alibaba.graphscope.groot.service.models.VertexRequest;


// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.http.HttpStatus;
// import org.springframework.http.MediaType;
// import org.springframework.http.ResponseEntity;
// import org.springframework.stereotype.Controller;
// import org.springframework.web.bind.annotation.PathVariable;
// import org.springframework.web.bind.annotation.RequestBody;
// import org.springframework.web.bind.annotation.RequestHeader;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.CookieValue;
// import org.springframework.web.bind.annotation.RequestParam;
// import org.springframework.web.bind.annotation.RequestPart;
// import org.springframework.web.multipart.MultipartFile;
// import org.springframework.web.context.request.NativeWebRequest;

// import javax.validation.constraints.*;
// import javax.validation.Valid;

// import java.util.List;
// import java.util.Map;
// import java.util.Optional;
// import javax.annotation.Generated;

// @Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2024-12-17T16:29:16.306086+08:00[Asia/Shanghai]")
// @Controller
// @RequestMapping("${openapi.graphScopeInteractiveAPIV03.base-path:/GRAPHSCOPE/InteractiveAPI/1.0.0}")
// public class V1ApiController implements V1Api {

//     private final NativeWebRequest request;

//     @Autowired
//     public V1ApiController(NativeWebRequest request) {
//         this.request = request;
//     }

//     @Override
//     public Optional<NativeWebRequest> getRequest() {
//         return Optional.ofNullable(request);
//     }

//     @Override
//     @RequestMapping(value = "/v1/graph/{graph_id}/adhoc_query",
//                  produces = { "text/plain", "application/json" },
//                  consumes = { "text/plain" })
//     public ResponseEntity<byte[]> runAdhoc(
//             @PathVariable("graph_id") String graphId,
//             @RequestBody(required = false) byte[] body) {
//         System.out.println("test 1111111111111111111111 runAdhoc");
//         return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
//     }
// }
