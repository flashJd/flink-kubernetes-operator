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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Autoscaling Example. */
public class MemoryTuneExample {
    public static void main(String[] args) throws Exception {

        ConfigOption<Boolean> cacheEnable =
                ConfigOptions.key("example.cache.data").booleanType().defaultValue(false);
        ConfigOption<Integer> dataCountOption =
                ConfigOptions.key("example.cache.number").intType().defaultValue(10240000);

        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/config/#rocksdb-native-metrics
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        boolean enableCacheLong = env.getConfiguration().get(cacheEnable);
        int dataCount = env.getConfiguration().get(dataCountOption);

        DataStream<Long> stream = env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE);
        stream.keyBy(t -> t)
                .process(
                        new KeyedProcessFunction<Long, Long, Long>() {

                            private final Logger logger = LoggerFactory.getLogger(this.getClass());

                            Set<Long> cacheValues = new HashSet<>();

                            int totalCount = dataCount;
                            int count = 0;

                            private ValueState<Integer> state;

                            private MemoryPoolMXBean firstPool;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                ValueStateDescriptor<Integer> desctiptor =
                                        new ValueStateDescriptor<>("distinct", Integer.class);
                                state = getRuntimeContext().getState(desctiptor);

                                final List<MemoryPoolMXBean> memoryPoolMXBeans =
                                        ManagementFactory.getMemoryPoolMXBeans().stream()
                                                .filter(bean -> "Metaspace".equals(bean.getName()))
                                                .collect(Collectors.toList());

                                final Iterator<MemoryPoolMXBean> beanIterator =
                                        memoryPoolMXBeans.iterator();
                                this.firstPool = beanIterator.next();
                            }

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

                                if (enableCacheLong) {
                                    if (count++ < totalCount) {
                                        cacheValues.add(value);
                                        if (count % 10000 == 0) {
                                            printMemUsage();
                                        }
                                    }
                                }
                            }

                            private void printMemUsage() {
                                MemoryUsage memoryUsage = this.firstPool.getUsage();
                                logger.info(
                                        "Memory usage: {}/{}, count={}",
                                        memoryUsage.getUsed(),
                                        memoryUsage.getMax(),
                                        count);
                            }
                        })
                .addSink(
                        new SinkFunction<Long>() {
                            @Override
                            public void invoke(Long value, Context context) throws Exception {
                                // Do nothing
                            }
                        });
        env.execute("Memory tune Example");
    }
}
