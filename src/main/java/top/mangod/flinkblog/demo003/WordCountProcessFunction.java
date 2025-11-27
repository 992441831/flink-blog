package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例：每5分钟统计一次单词的数量
 **/
public class WordCountProcessFunction {
    /**
     * 基于Flink的流处理单词计数应用
     * 使用事件时间滚动窗口结合reduce和ProcessFunction实现的单词统计
     * 包含增量聚合和最终合计处理的混合模式
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
                // 使用TumblingEventTimeWindows创建10秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                
                // 使用Lambda表达式实现增量聚合
                // 对每个单词组内的数据进行累加，具有更好的性能（内存高效）
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))
                
                // 使用ProcessFunction进行最终处理
                // 这里将计算结果的数量翻倍并以"合计"为key输出
                // 注意：代码中存在逻辑，value.f1 += value.f1等价于value.f1 * 2
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, 
                                             ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, 
                                             Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 将当前单词的计数值翻倍
                        value.f1 += value.f1;
                        // 以"合计"作为key输出处理结果
                        out.collect(new Tuple2<>("合计", value.f1));
                    }
                });
    
        // 4. 数据输出
        // 将单词计数结果打印到控制台，并设置并行度为1确保输出顺序性
        // 注意：输出前缀仍为"WindowWordCount03 ======= "，可能是复制代码时未修改
        wordCountStream.print("WindowWordCount03 ======= ").setParallelism(1);
        
        // 5. 启动任务
        // 执行Flink作业，以类名作为作业名称，方便在监控系统中识别
        env.execute(WordCountProcessFunction.class.getSimpleName());
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