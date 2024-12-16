package com.alibaba.graphscope.groot.service;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.alibaba.graphscope.groot.sdk.schema.Vertex;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/v1/graph")
public class GrootController {

    private final GrootService grootService;

    @Autowired
    public GrootController(GrootService grootService) {
        this.grootService = grootService;
    }
    
    @PostMapping("/vertex")
    public ResponseEntity<Long> addVertex(@RequestBody Vertex vertex) {
        long id = grootService.addVertex(vertex);
        return ResponseEntity.ok(id);
    }

    @PostMapping("/query/gremlin")
    public ResponseEntity<List<Result>> executeGremlinQuery(@RequestBody String query) throws ExecutionException, InterruptedException {
        List<Result> results = grootService.executeGremlinQuery(query).get();
        return ResponseEntity.ok(results);
    }
}
