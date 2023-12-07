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

package org.apache.flink.autoscaler.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nonnull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_PROCESS_MEMORY;

/**
 * The state store is responsible for storing all state during scaling.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of JobAutoScalerContext.
 */
@Experimental
public interface AutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>> {

    void storeScalingHistory(
            Context jobContext, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory)
            throws Exception;

    @Nonnull
    Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(Context jobContext)
            throws Exception;

    void storeScalingTracking(Context jobContext, ScalingTracking scalingTrack) throws Exception;

    ScalingTracking getScalingTracking(Context jobContext) throws Exception;

    void removeScalingHistory(Context jobContext) throws Exception;

    void storeCollectedMetrics(Context jobContext, SortedMap<Instant, CollectedMetrics> metrics)
            throws Exception;

    @Nonnull
    SortedMap<Instant, CollectedMetrics> getCollectedMetrics(Context jobContext) throws Exception;

    void removeCollectedMetrics(Context jobContext) throws Exception;

    void storeParallelismOverrides(Context jobContext, Map<String, String> parallelismOverrides)
            throws Exception;

    void storeProcessMemOverrides(Context jobContext, String processMem) throws Exception;

    void storeManagedMemOverrides(Context jobContext, String managed) throws Exception;

    Optional<String> getProcessMemOverrides(Context jobContext);

    Optional<String> getManagedMemOverrides(Context jobContext);

    default Map<String, String> getMemOverrides(Context jobContext) throws Exception {
        Map<String, String> memInfo = new HashMap<>(2);
        getProcessMemOverrides(jobContext).ifPresent(v -> memInfo.put(TOTAL_PROCESS_MEMORY.key(), v));
        getManagedMemOverrides(jobContext).ifPresent(v -> memInfo.put(MANAGED_MEMORY_SIZE.key(), v));
        return memInfo;
    }

    @Nonnull
    Map<String, String> getParallelismOverrides(Context jobContext) throws Exception;

    void removeParallelismOverrides(Context jobContext) throws Exception;

    /** Removes all data from this context. Flush stil needs to be called. */
    void clearAll(Context jobContext) throws Exception;

    /**
     * Flushing is needed because we do not persist data for all store methods until this method is
     * called. Note: The state store implementation should try to avoid write operations unless data
     * was changed through this interface.
     */
    void flush(Context jobContext) throws Exception;

    /** Clean up all information related to the current job. */
    void removeInfoFromCache(KEY jobKey);
}
