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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.EXCLUDED_PERIODS;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_MANAGED_HIT_RATIO;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_MANAGED_MEM_BOAST_RATIO;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_EXECUTION_DISABLED_REASON;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_USAGE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.ROCKSDB_CACHE_HIT_RATIO;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_FRACTION;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_PROCESS_MEMORY;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor<KEY, Context extends JobAutoScalerContext<KEY>> {

    public static final String GC_PRESSURE_MESSAGE =
            "GC Pressure %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    public static final String HEAP_USAGE_MESSAGE =
            "Heap Usage %s is above the allowed limit for scaling operations. Please adjust the available memory manually.";

    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final JobVertexScaler<KEY, Context> jobVertexScaler;
    private final AutoScalerEventHandler<KEY, Context> autoScalerEventHandler;
    private final AutoScalerStateStore<KEY, Context> autoScalerStateStore;

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this(
                new JobVertexScaler<>(autoScalerEventHandler),
                autoScalerEventHandler,
                autoScalerStateStore);
    }

    public ScalingExecutor(
            JobVertexScaler<KEY, Context> jobVertexScaler,
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this.jobVertexScaler = jobVertexScaler;
        this.autoScalerEventHandler = autoScalerEventHandler;
        this.autoScalerStateStore = autoScalerStateStore;
    }

    public boolean scaleResource(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            ScalingTracking scalingTracking,
            Instant now)
            throws Exception {
        var conf = context.getConfiguration();
        var scaleEnabled = conf.get(SCALING_ENABLED);
        var restartTime = scalingTracking.getMaxRestartTimeOrDefault(conf);

        var memoryPressure = isJobUnderMemoryPressure(context, evaluatedMetrics.getGlobalMetrics());
        var scalingSummaries =
                computeScalingSummary(
                        context, evaluatedMetrics, scalingHistory, restartTime, memoryPressure);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        if (allVerticesWithinUtilizationTarget(
                evaluatedMetrics.getVertexMetrics(), scalingSummaries)) {
            return false;
        }

        updateRecommendedParallelism(evaluatedMetrics.getVertexMetrics(), scalingSummaries);

        if (checkIfBlockedAndTriggerScalingEvent(context, scalingSummaries, conf, now)) {
            return false;
        }

        if (scaleEnabled) {
            boastJobMemory(context, evaluatedMetrics, memoryPressure);
        }

        addToScalingHistoryAndStore(
                autoScalerStateStore, context, scalingHistory, now, scalingSummaries);

        scalingTracking.addScalingRecord(now, new ScalingRecord());
        autoScalerStateStore.storeScalingTracking(context, scalingTracking);

        autoScalerStateStore.storeParallelismOverrides(
                context,
                getVertexParallelismOverrides(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries));

        return true;
    }

    public boolean boastJobMemory(
            Context context, EvaluatedMetrics evaluatedMetrics, boolean memoryPressure)
            throws Exception {
        if (memoryPressure) {
            autoScalerStateStore.setMemoryUnderPressure(context);
            return true;
        } else {
            autoScalerStateStore.removeMemoryUnderPressure(context);
        }

        var conf = context.getConfiguration();
        var cacheHitRate = evaluatedMetrics.getGlobalMetrics().get(ROCKSDB_CACHE_HIT_RATIO);
        if (cacheHitRate != null && cacheHitRate.getAverage() < conf.get(FLINK_MANAGED_HIT_RATIO)) {
            var managedMem = conf.get(MANAGED_MEMORY_SIZE);
            if (managedMem == null) {
                var managedFraction = conf.get(MANAGED_MEMORY_FRACTION);
                var totalMemory = conf.get(TOTAL_PROCESS_MEMORY);
                managedMem = totalMemory.multiply(managedFraction);
            }

            MemorySize totalHeap =
                    TaskExecutorProcessUtils.processSpecFromConfig(conf)
                            .getFlinkMemory()
                            .getTaskHeap();
            double heapUsed = evaluatedMetrics.getGlobalMetrics().get(HEAP_USAGE).getAverage();
            MemorySize freeHeap = totalHeap.multiply(1 - heapUsed);
            float boastRatio = conf.get(FLINK_MANAGED_MEM_BOAST_RATIO);
            assert boastRatio > 0 && boastRatio < 1;
            var newManagedMem = managedMem.add(freeHeap.multiply(boastRatio));
            autoScalerStateStore.storeManagedMemOverrides(
                    context, newManagedMem.getMebiBytes() + "m");
            LOG.info(
                    "Scaling {} managed from {} to {} due to freeHeap {}",
                    context.getJobKey(),
                    managedMem,
                    newManagedMem,
                    freeHeap);
            return true;
        }
        return false;
    }

    private void updateRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        scalingSummaries.forEach(
                (jobVertexID, scalingSummary) ->
                        evaluatedMetrics
                                .get(jobVertexID)
                                .put(
                                        ScalingMetric.RECOMMENDED_PARALLELISM,
                                        EvaluatedScalingMetric.of(
                                                scalingSummary.getNewParallelism())));
    }

    protected static boolean allVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            var vertex = entry.getKey();
            var metrics = evaluatedMetrics.get(vertex);

            double processingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (processingRate < scaleUpRateThreshold || processingRate > scaleDownRateThreshold) {
                LOG.debug(
                        "Vertex {} processing rate {} is outside ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {} processing rate {} is within target ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }

    @VisibleForTesting
    Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Duration restartTime,
            boolean memoryPressure) {

        if (memoryPressure) {
            LOG.info("Skipping vertex scaling due to memory pressure");
            return Map.of();
        }

        var out = new HashMap<JobVertexID, ScalingSummary>();
        var excludeVertexIdList =
                context.getConfiguration().get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics
                .getVertexMetrics()
                .forEach(
                        (v, metrics) -> {
                            if (excludeVertexIdList.contains(v.toHexString())) {
                                LOG.debug(
                                        "Vertex {} is part of `vertex.exclude.ids` config, Ignoring it for scaling",
                                        v);
                            } else {
                                var currentParallelism =
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();
                                var newParallelism =
                                        jobVertexScaler.computeScaleTargetParallelism(
                                                context,
                                                v,
                                                metrics,
                                                scalingHistory.getOrDefault(
                                                        v, Collections.emptySortedMap()),
                                                restartTime);
                                if (currentParallelism != newParallelism) {
                                    out.put(
                                            v,
                                            new ScalingSummary(
                                                    currentParallelism, newParallelism, metrics));
                                }
                            }
                        });
        return out;
    }

    private boolean isJobUnderMemoryPressure(
            Context ctx, Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics) {

        var gcPressure = evaluatedMetrics.get(ScalingMetric.GC_PRESSURE).getCurrent();
        var conf = ctx.getConfiguration();
        if (gcPressure > conf.get(AutoScalerOptions.GC_PRESSURE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(GC_PRESSURE_MESSAGE, gcPressure),
                    "gcPressure",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        var heapUsage = evaluatedMetrics.get(ScalingMetric.HEAP_USAGE).getAverage();
        if (heapUsage > conf.get(AutoScalerOptions.HEAP_USAGE_THRESHOLD)) {
            autoScalerEventHandler.handleEvent(
                    ctx,
                    AutoScalerEventHandler.Type.Normal,
                    "MemoryPressure",
                    String.format(HEAP_USAGE_MESSAGE, heapUsage),
                    "heapUsage",
                    conf.get(SCALING_EVENT_INTERVAL));
            return true;
        }

        return false;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        return overrides;
    }

    private boolean checkIfBlockedAndTriggerScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Configuration conf,
            Instant now) {
        var scaleEnabled = conf.get(SCALING_ENABLED);
        var isExcluded = CalendarUtils.inExcludedPeriods(conf, now);
        String message;
        if (!scaleEnabled) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    SCALING_ENABLED.key(),
                                    false);
        } else if (isExcluded) {
            message =
                    SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                            + String.format(
                                    SCALING_EXECUTION_DISABLED_REASON,
                                    EXCLUDED_PERIODS.key(),
                                    conf.get(EXCLUDED_PERIODS));
        } else {
            message = SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
        }
        autoScalerEventHandler.handleScalingEvent(
                context, scalingSummaries, message, conf.get(SCALING_EVENT_INTERVAL));

        return !scaleEnabled || isExcluded;
    }
}
