package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例：每5分钟统计一次单词的数量
 **/
public class WordCountProcessWindowFunction {
    /**
     * 基于Flink的流处理单词计数应用
     * 使用事件时间滚动窗口(EventTime Tumbling Window)和ProcessWindowFunction实现
     * 适用于需要访问窗口上下文信息的场景
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
    
        // 3. 数据转换
        // 对读取的文本流进行一系列转换操作，最终生成单词计数结果
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                // 分配时间戳和水印，使用自定义的MyWatermark水印策略
                // 水印用于处理事件时间和乱序数据
                .assignTimestampsAndWatermarks(MyWatermark.create())
                
                // 对数据源的单词进行拆分，每个单词记为1
                // 通过FlatMapFunction将每行文本拆分为多个(word, 1)键值对
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                 // 使用正则表达式\s(空白字符)分割输入字符串
                                 for (String word : value.split("\\s")) {
                                     // 将每个单词与计数1组合成元组并发送到下游
                                     out.collect(new Tuple2<>(word, 1));
                                 }
                             }
                         }
                )
                
                // 对单词进行分组
                // 使用keyBy基于元组的第一个字段(word)进行分组
                .keyBy(value -> value.f0)
                
                // 设置滚动事件时间窗口
                // 使用TumblingEventTimeWindows创建5秒的滚动窗口
                // 滚动窗口不重叠，每个窗口严格独立
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                
                // 使用ProcessWindowFunction处理窗口数据
                // ProcessWindowFunction提供更强大的功能，可以访问窗口上下文信息
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, 
                                       Context context, 
                                       Iterable<Tuple2<String, Integer>> elements, 
                                       Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 初始化计数器
                        int sum = 0;
                        
                        // 遍历窗口中的所有元素并累加计数
                        for (Tuple2<String, Integer> element : elements) {
                            sum += element.f1;
                        }
                        
                        // 收集处理结果：单词(key)和总计数(sum)
                        out.collect(new Tuple2<>(key, sum));
                    }
                });
    
        // 4. 数据输出
        // 将单词计数结果打印到控制台，并设置并行度为1确保输出顺序性
        // 前缀"WindowWordCount03 ======= "用于区分不同作业的输出
        wordCountStream.print("WindowWordCount03 ======= ").setParallelism(1);
        
        // 5. 启动任务
        // 执行Flink作业，以类名作为作业名称，方便在监控系统中识别
        env.execute(WordCountProcessWindowFunction.class.getSimpleName());
    }

    private static class MyWatermark<T> implements WatermarkStrategy<T> {

        private MyWatermark() {
        }

        public static <T> MyWatermark<T> create() {
            return new MyWatermark<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}