package com.alibaba.graphscope.groot.service.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

public class ApiUtil {
    public static void setExampleResponse(
            NativeWebRequest req, String contentType, String example) {
        try {
            HttpServletResponse res = req.getNativeResponse(HttpServletResponse.class);
            res.setCharacterEncoding("UTF-8");
            res.addHeader("Content-Type", contentType);
            res.getWriter().print(example);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ResponseEntity<String> createErrorResponse(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(String.format("{\"error\": \"%s\"}", message));
    }

    public static ResponseEntity<String> createSuccessResponse(String message, long snapshotId) {
        return ResponseEntity.status(HttpStatus.OK)
                .body(
                        String.format(
                                "{\"message\": \"%s\", \"snapshot_id\": %d}", message, snapshotId));
    }

    public static ResponseEntity<String> createSuccessResponse(String message) {
        return ResponseEntity.status(HttpStatus.OK)
                .body(String.format("{\"message\": \"%s\"}", message));
    }
}
