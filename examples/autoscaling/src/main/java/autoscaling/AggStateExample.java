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

package autoscaling;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.configuration.DeploymentOptions.TARGET;

/** Autoscaling Example. */
public class AggStateExample {
    public static void main(String[] args) throws Exception {

        //
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/config/#rocksdb-native-metrics
        Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/scaling/checkpoint");
        configuration.set(CheckpointingOptions.STATE_BACKEND, "rocksdb");
        //        configuration.set(CheckpointingOptions.STATE_BACKEND, "hashmap");
        configuration.set(RestOptions.PORT, 8083);
        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(15));
        configuration.set(
                ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60));

        configuration.set(RocksDBNativeMetricOptions.BLOCK_CACHE_PINNED_USAGE, true);
        configuration.set(RocksDBNativeMetricOptions.MONITOR_BYTES_WRITTEN, true);
        configuration.set(RocksDBNativeMetricOptions.MONITOR_BLOCK_CACHE_HIT, true);
        configuration.set(RocksDBNativeMetricOptions.MONITOR_BLOCK_CACHE_MISS, true);
        configuration.set(RocksDBNativeMetricOptions.MONITOR_STALL_MICROS, true);
        //        configuration.set(TASK_OFF_HEAP_MEMORY, MemorySize.parse("256m"));
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("10m"));

        configuration.set(TARGET, "remote");

        //        configuration.set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);

        var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        DataStream<Long> stream = env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE);
        stream.keyBy(t -> t)
                .process(
                        new KeyedProcessFunction<Long, Long, Long>() {

                            @Override
                            public void processElement(
                                    Long value,
                                    KeyedProcessFunction<Long, Long, Long>.Context context,
                                    Collector<Long> out)
                                    throws Exception {
                                if (state.value() == null) {
                                    state.update(1);
                                    out.collect(value);
                                }
                            }

                            private ValueState<Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                ValueStateDescriptor<Integer> desctiptor =
                                        new ValueStateDescriptor<>("distinct", Integer.class);
                                state = getRuntimeContext().getState(desctiptor);
                            }
                        })
                .addSink(
                        new SinkFunction<Long>() {
                            @Override
                            public void invoke(Long value, Context context) throws Exception {
                                // Do nothing
                            }
                        });
        //        stream.shuffle()
        env.execute("Autoscaling Example");
    }
}
