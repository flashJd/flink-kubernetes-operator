/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.*;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.rest.messages.taskmanager.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Metric collector using flink rest api. */
public class RestApiMetricsCollector<KEY, Context extends JobAutoScalerContext<KEY>>
        extends ScalingMetricCollector<KEY, Context> {
    private static final Logger LOG = LoggerFactory.getLogger(RestApiMetricsCollector.class);

    @Override
    protected Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            Context ctx, Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {

        return filteredVertexMetricNames.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e -> queryAggregatedVertexMetrics(ctx, e.getKey(), e.getValue())));
    }

    @Override
    @SneakyThrows
    protected Map<ResourceID, Tuple2<Long, Long>> queryAllTaskManagerResourceProfiles(Context ctx) {
        try (var restClient = ctx.getRestClusterClient()){
            var taskManagersInfo = restClient.sendRequest(
                    TaskManagersHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance()).get();
            var tms = taskManagersInfo.getTaskManagerInfos();
            Map<ResourceID, Tuple2<Long, Long>> resourceProfiles = new HashMap(tms.size());
            for (var taskManager : tms) {
                var parameters = new TaskManagerMetricsMessageParameters();
                var pathIt = parameters.getPathParameters().iterator();
                ((TaskManagerIdPathParameter)pathIt.next()).resolve(taskManager.getResourceId());

                var taskManagerDetail = restClient.sendRequest(TaskManagerDetailsHeaders.getInstance(),
                        parameters, EmptyRequestBody.getInstance()).get();
                var tmMetric = taskManagerDetail.getTaskManagerMetricsInfo();
                long heapUsed = getMetricItem(tmMetric, "heapUsed");
                long heapMax = getMetricItem(tmMetric, "heapMax");
                resourceProfiles.put(taskManager.getResourceId(),
                        Tuple2.of(heapMax, heapMax-heapUsed));
            }
            return resourceProfiles;
        }
    }

    private long getMetricItem(TaskManagerMetricsInfo metricsInfo, String item)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = TaskManagerMetricsInfo.class.getDeclaredField(item);
        field.setAccessible(true);
        return (long) field.get(metricsInfo);
    }

    @SneakyThrows
    protected Map<FlinkMetric, AggregatedMetric> queryAggregatedVertexMetrics(
            Context ctx, JobVertexID jobVertexID, Map<String, FlinkMetric> metrics) {

        LOG.debug("Querying metrics {} for {}", metrics, jobVertexID);

        var parameters = new AggregatedSubtaskMetricsParameters();
        var pathIt = parameters.getPathParameters().iterator();

        ((JobIDPathParameter) pathIt.next()).resolve(ctx.getJobID());
        ((JobVertexIdPathParameter) pathIt.next()).resolve(jobVertexID);

        parameters
                .getQueryParameters()
                .iterator()
                .next()
                .resolveFromString(StringUtils.join(metrics.keySet(), ","));

        try (var restClient = ctx.getRestClusterClient()) {

            var responseBody =
                    restClient
                            .sendRequest(
                                    AggregatedSubtaskMetricsHeaders.getInstance(),
                                    parameters,
                                    EmptyRequestBody.getInstance())
                            .get();

            return responseBody.getMetrics().stream()
                    .collect(
                            Collectors.toMap(
                                    m -> metrics.get(m.getId()),
                                    m -> m,
                                    (m1, m2) ->
                                            new AggregatedMetric(
                                                    m1.getId() + " merged with " + m2.getId(),
                                                    Math.min(m1.getMin(), m2.getMin()),
                                                    Math.max(m1.getMax(), m2.getMax()),
                                                    // Average can't be computed
                                                    Double.NaN,
                                                    m1.getSum() + m2.getSum())));
        }
    }
}
