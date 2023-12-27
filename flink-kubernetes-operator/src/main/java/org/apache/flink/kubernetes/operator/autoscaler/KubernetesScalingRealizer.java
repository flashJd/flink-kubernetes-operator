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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.api.spec.Resource;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_JOB_MEM_BOAST_RATIO;
import static org.apache.flink.kubernetes.operator.utils.FlinkUtils.getTaskManagerSpec;

/** The Kubernetes implementation for applying parallelism overrides. */
public class KubernetesScalingRealizer
        implements ScalingRealizer<ResourceID, KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesScalingRealizer.class);

    @Override
    public void realize(
            KubernetesJobAutoScalerContext context, Map<String, String> parallelismOverrides) {

        context.getResource()
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                        getOverrideString(context, parallelismOverrides));
    }

    @Override
    public void rescaleMemory(
            KubernetesJobAutoScalerContext context,
            boolean underMemoryPressure,
            Map<String, String> memoryInfo) {
        var autoScaleConf = context.getConfiguration();
        if (underMemoryPressure) {
            var taskManagerSpec = getTaskManagerSpec(context);
            var originResource = taskManagerSpec.getResource();
            var originMem = MemorySize.parse(originResource.getMemory());
            double jobMemBoastRatio = autoScaleConf.get(FLINK_JOB_MEM_BOAST_RATIO);
            var newMem = originMem.multiply(1 + jobMemBoastRatio);
            var newResource =
                    new Resource(
                            originResource.getCpu(),
                            newMem.getMebiBytes() + "m",
                            originResource.getEphemeralStorage());
            LOG.info(
                    "Rescale {} memory from {} to {} due to jobMemBoastRatio {}",
                    context.getJobKey(),
                    originMem,
                    newMem,
                    jobMemBoastRatio);
            taskManagerSpec.setResource(newResource);
        }

        memoryInfo.forEach(
                (k, v) -> {
                    autoScaleConf.setString(k, v);
                    context.getResource().getSpec().getFlinkConfiguration().put(k, v);
                });
    }

    @Nullable
    private static String getOverrideString(
            KubernetesJobAutoScalerContext context, Map<String, String> newOverrides) {
        if (context.getResource().getStatus().getReconciliationStatus().isBeforeFirstDeployment()) {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }

        var conf = context.getResourceContext().getObserveConfig();
        var currentOverrides =
                conf.getOptional(PipelineOptions.PARALLELISM_OVERRIDES).orElse(Map.of());

        // Check that the overrides actually changed and not just the String representation.
        // This way we prevent reconciling a NOOP config change which would unnecessarily redeploy
        // the pipeline.
        if (currentOverrides.equals(newOverrides)) {
            // If overrides are identical, use the previous string as-is.
            return conf.getValue(PipelineOptions.PARALLELISM_OVERRIDES);
        } else {
            return ConfigurationUtils.convertValue(newOverrides, String.class);
        }
    }
}
