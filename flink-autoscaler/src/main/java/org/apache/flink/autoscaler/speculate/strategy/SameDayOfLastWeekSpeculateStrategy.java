/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.speculate.strategy;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/** The speculative strategy refer to the same day of last week. */
public class SameDayOfLastWeekSpeculateStrategy implements SpeculativeStrategy {

    private static final Logger LOG =
            LoggerFactory.getLogger(SameDayOfLastWeekSpeculateStrategy.class);

    @Override
    public String strategyName() {
        return SameDayOfLastWeekSpeculateStrategy.class.getName();
    }

    @Override
    public <KEY, Context extends JobAutoScalerContext<KEY>>
            boolean triggerSpeculativeScalingExecution(
                    AutoScalerStateStore<KEY, Context> autoScalerStateStore,
                    Context context,
                    Instant now) {
        try {
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory =
                    autoScalerStateStore.getScalingHistory(context);
            Instant lastWeek = now.minus(7, ChronoUnit.DAYS);
            Instant instantToRefer;
            for (Map.Entry<JobVertexID, SortedMap<Instant, ScalingSummary>> entry :
                    scalingHistory.entrySet()) {
                Instant instant = entry.getValue().lastKey();
                ScalingSummary scalingSummary = entry.getValue().get(instant);
                Instant instantOneWeekAgo = entry.getValue().headMap(lastWeek).lastKey();
                ScalingSummary scalingSummaryOneWeekAgo = entry.getValue().get(instantOneWeekAgo);
                if (scalingSummary.getCurrentParallelism()
                                != scalingSummaryOneWeekAgo.getCurrentParallelism()
                        || scalingSummary.getNewParallelism()
                                != scalingSummaryOneWeekAgo.getNewParallelism()) {
                    LOG.warn(
                            "Do not trigger same day of last week speculative execution because the precondition: current parallelism and new parallelism must be same is not satisfied, jobVertexId {}, "
                                    + "today's last summary: current parallelism {} new parallelism {}, lastWeek's summary: current parallelism {} new parallelism {}.",
                            entry.getKey(),
                            scalingSummary.getCurrentParallelism(),
                            scalingSummary.getNewParallelism(),
                            scalingSummaryOneWeekAgo.getCurrentParallelism(),
                            scalingSummaryOneWeekAgo.getNewParallelism());
                    return false;
                }
            }

            Map<String, String> parallelismOverrides = new HashMap<>();
            Map<JobVertexID, ScalingSummary> scalingHistoryToAdd = new HashMap<>();
            for (Map.Entry<JobVertexID, SortedMap<Instant, ScalingSummary>> entry :
                    scalingHistory.entrySet()) {
                instantToRefer = entry.getValue().tailMap(lastWeek).firstKey();
                if (instantToRefer.plus(7, ChronoUnit.DAYS).isAfter(now.plusSeconds(10 * 60))) {
                    LOG.warn(
                            "Do not trigger same day of last week speculative execution because expected advance has not been reached, jobVertexId {}, "
                                    + "expected advance instant {}, current advance instant {}",
                            entry.getKey(),
                            instantToRefer.plus(7, ChronoUnit.DAYS),
                            now.plusSeconds(10 * 60));
                    return false;
                }

                ScalingSummary scalingSummary = entry.getValue().get(instantToRefer);
                if (scalingSummary.getNewParallelism() < scalingSummary.getCurrentParallelism()) {
                    LOG.warn(
                            "Do not trigger same day of last week speculative execution because last week's scalingSummary to refer to has scaleDown operation, jobVertexId {}, "
                                    + "scalingSummary to refer to {}",
                            entry.getKey(),
                            scalingSummary);
                    return false;
                }
                parallelismOverrides.put(
                        entry.getKey().toString(),
                        String.valueOf(scalingSummary.getNewParallelism()));
                scalingHistoryToAdd.put(entry.getKey(), scalingSummary);
            }

            for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingHistoryToAdd.entrySet()) {
                scalingHistory.get(entry.getKey()).put(now, entry.getValue());
            }

            autoScalerStateStore.storeScalingHistory(context, scalingHistory);
            autoScalerStateStore.storeParallelismOverrides(context, parallelismOverrides);
            LOG.info(
                    "Trigger same day of last week speculative execution, parallelismOverrides {}",
                    parallelismOverrides);
            return true;
        } catch (Exception e) {
            LOG.error("Speculative execution error.", e);
            return false;
        }
    }
}
