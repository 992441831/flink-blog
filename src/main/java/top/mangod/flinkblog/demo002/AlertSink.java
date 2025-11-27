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

package top.mangod.flinkblog.demo002;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义sink，用于输出学生对象
 * 实现了Flink的SinkFunction<SourceSourceDemo.Student>接口，专门用于处理和输出Student类型的数据
 * 作为Flink流处理管道的终点，接收上游处理完成的数据并进行最终输出
 * 这里简单地将Student对象转换为字符串并打印到日志中，实际应用中可以根据需求进行自定义输出逻辑，例如写入数据库、消息队列等
 */
@PublicEvolving
@SuppressWarnings("unused")
public class AlertSink implements SinkFunction<SourceSourceDemo.Student> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    @Override
    public void invoke(SourceSourceDemo.Student value, Context context) {
        LOG.info("自定义sink" + value.toString());
    }
}
