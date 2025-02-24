/*
 * Copyright 2025 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
