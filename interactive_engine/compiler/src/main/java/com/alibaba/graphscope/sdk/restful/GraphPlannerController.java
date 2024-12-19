/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.sdk.restful;

import com.alibaba.graphscope.sdk.GraphPlan;
import com.alibaba.graphscope.sdk.PlanUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/api")
public class GraphPlannerController {
    private final Logger logger = LoggerFactory.getLogger(GraphPlannerController.class);

    @PostMapping("/compilePlan")
    public ResponseEntity<GraphPlanResponse> compilePlan(@RequestBody GraphPlanRequest request) {
        try {
            GraphPlan plan =
                    PlanUtils.compilePlan(
                            request.getConfigPath(),
                            request.getQuery(),
                            request.getSchemaYaml(),
                            request.getStatsJson());
            return ResponseEntity.ok(new GraphPlanResponse(plan));
        } catch (Exception e) {
            logger.error("Failed to compile plan", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new GraphPlanResponse(e.getMessage()));
        }
    }
}
