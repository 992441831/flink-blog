package top.mangod.flinkblog.demo004;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 **/
public class LateEventsSocketWindowWordCount {
    private static final OutputTag<Tuple2<String, Integer>> lateEventsTag =
            new OutputTag<Tuple2<String, Integer>>("late-events") {
            };

    /**
     * 基于Flink的延迟事件处理示例应用
     * 演示了如何处理具有事件时间和延迟数据的流处理场景
     * 使用水印(Watermark)、允许延迟处理和侧输出(Side Output)来处理迟到的数据
     * @param args 命令行参数
     * @throws Exception 执行过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        // 获取Flink流处理的执行环境，这是所有Flink应用的入口点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        // 2. 读取数据源
        // 从本地9999端口读取Socket文本流，以换行符("\n")作为数据分隔符
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
    
        // 3. 数据转换和处理
        // 例如输入格式：1000,a   2000,a  3000,b （时间戳，单词）
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowCountStream = textStream
                // 4. 设置水印策略
                // 创建允许2秒无序度的水印策略，处理乱序事件
                // 使用withTimestampAssigner从输入数据中提取事件时间（每行第一个字段作为时间戳）
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) ->
                                        Long.parseLong(event.split(",")[0])))
                
                // 5. 数据格式转换
                // 将输入字符串转换为Tuple2<单词, 1>格式
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        // 按逗号分割输入数据
                        String[] splits = value.split(",");
                        // 返回单词(第二个字段)和计数1
                        return Tuple2.of(splits[1], 1);
                    }
                })
                
                // 6. 按单词分组
                .keyBy(value -> value.f0)
                
                // 7. 设置滚动事件时间窗口
                // 创建5秒的滚动窗口（实际代码注释写的是5分钟，但实现是5秒）
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                
                // 8. 设置窗口允许的延迟时间
                // 窗口关闭后，再保留2秒用于处理迟到的数据
                .allowedLateness(Time.seconds(2))
                
                // 9. 设置侧输出标签
                // 将超过allowedLateness时间的迟到数据发送到侧输出
                .sideOutputLateData(lateEventsTag)
                
                // 10. 窗口计算函数
                // 使用WindowFunction处理窗口内的数据
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key,          // 分组的键（单词）
                                      TimeWindow window,   // 当前窗口对象
                                      Iterable<Tuple2<String, Integer>> input,  // 窗口内的所有元素
                                      Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 此处只输出第一个元素，在实际应用中可能需要进行聚合计算
                        // 注意：这种实现方式对于多个相同key的数据只保留一个
                        out.collect(input.iterator().next());
                    }
                });
    
        // 11. 数据输出 - 正常数据
        // 打印正常处理的元素，设置并行度为1确保输出顺序性
        windowCountStream.print("正常数据 ========== ").setParallelism(1);
    
        // 12. 数据输出 - 延迟数据
        // 从侧输出中获取并打印迟到的数据
        DataStream<Tuple2<String, Integer>> lateEvents = windowCountStream.getSideOutput(lateEventsTag);
        lateEvents.print("延迟侧道数据 ========== ").setParallelism(1);
    
        // 13. 启动任务
        // 执行Flink作业，以类名作为作业名称
        env.execute(LateEventsSocketWindowWordCount.class.getSimpleName());
    }
}