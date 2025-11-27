package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 当程序运行时，它会持续监听本地9999端口的输入，每5秒钟对这个窗口内收到的所有单词进行统计计数，
 * 并将结果输出到控制台。这是一个典型的实时流处理应用场景，适用于日志分析、实时监控等需求
 **/
public class WordCountSlidingProcessingTimeWindows {
    /**
     * 主方法，实现基于Flink的窗口单词计数功能
     * 该方法创建一个实时流处理应用，从Socket读取数据，
     * 按5秒的滚动事件时间窗口统计单词频率，并输出结果
     * 
     * @param args 命令行参数，本示例未使用
     * @throws Exception 执行过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境 - 初始化Flink的流处理执行环境
        // 这是所有Flink应用的入口点，负责管理作业的生命周期和资源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 读取数据源 - 从本地Socket读取文本流
        // 连接到localhost的9999端口，以换行符作为分隔符
        // 这是一个无界数据源，持续监听并接收新的数据
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
    
        // 3. 数据转换 - 构建处理管道
        // 转换输入的文本流，执行单词拆分、分组、窗口计算和聚合操作
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                // 3.1 分配时间戳和水印 - 用于事件时间处理
                // 应用自定义的MyWatermark策略，为每个事件分配时间戳并生成水印
                // 水印用于处理乱序事件和确定窗口何时关闭
                .assignTimestampsAndWatermarks(MyWatermark.create())
                
                // 3.2 扁平化处理 - 将输入文本拆分为单词并计数
                // FlatMapFunction接收一个输入元素并可能产生多个输出元素
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                // 将输入字符串按空白字符拆分，得到单词数组
                                // 为每个单词创建一个(单词, 1)的元组
                                for (String word : value.split("\\s")) {
                                    // 通过Collector将处理后的结果发射到下游算子
                                    out.collect(new Tuple2<>(word, 1));
                                }
                            }
                        }
                )
                
                // 3.3 按键分组 - 按单词进行分组
                // 根据元组的第一个字段(即单词本身)进行分组
                // 确保相同单词的数据会被发送到同一个并行实例进行处理
                .keyBy(value -> value.f0)
                
                // 3.4 应用窗口 - 使用滚动事件时间窗口
                // 创建大小为5秒的滚动窗口，每个窗口相互不重叠
                // 基于事件时间进行窗口划分，而不是数据到达的处理时间
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                
                // 3.4.1 可选：使用滑动处理时间窗口
                // 如果需要更频繁地更新结果，可以使用滑动窗口
                // 这里配置的是5秒窗口大小，1秒滑动间隔
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                
                // 3.5 聚合计算 - 对窗口内的数据进行累加
                // reduce函数对每个窗口内同一单词的计数进行累加
                // a和b是相同单词的两个元组，将它们的计数值相加
                .reduce((a, b) ->
                        new Tuple2<>(a.f0, a.f1 + b.f1));
    
        // 4. 数据输出 - 将结果打印到控制台
        // 使用print操作将计算结果输出到控制台
        // 前缀"WindowWordCount01 ======= "用于标识输出
        // setParallelism(1)确保输出结果的全局顺序性
        wordCountStream.print("WindowWordCount01 ======= ").setParallelism(1);
        
        // 5. 启动任务 - 触发作业执行
        // 调用execute方法启动Flink作业
        // 传递作业名称用于监控和日志记录
        env.execute(WordCountSlidingProcessingTimeWindows.class.getSimpleName());
    }

    /**
     * 自定义水印策略类，实现WatermarkStrategy接口
     * 用于为Flink流处理应用提供时间戳分配和水印生成逻辑
     * 
     * @param <T> 泛型参数，表示可以处理任何类型的数据流
     */
    private static class MyWatermark<T> implements WatermarkStrategy<T> {
    
        /**
         * 私有构造函数
         * 确保只能通过静态工厂方法创建实例，遵循工厂模式设计
         */
        private MyWatermark() {
        }
    
        /**
         * 静态工厂方法，创建MyWatermark实例
         * 使用泛型方法参数，支持类型推断
         * 
         * @param <T> 数据类型
         * @return 返回一个新的MyWatermark实例
         */
        public static <T> MyWatermark<T> create() {
            return new MyWatermark<>();
        }
    
        /**
         * 创建水印生成器
         * 负责生成用于衡量事件时间进度的水印
         * 
         * @param context 水印生成器上下文，包含作业相关信息
         * @return 返回AscendingTimestampsWatermarks实例，适用于事件时间单调递增的场景
         *         该生成器会根据最大事件时间戳生成水印
         */
        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            // 使用Flink内置的AscendingTimestampsWatermarks
            // 该水印生成器适用于事件时间单调递增的数据流
            // 水印值被设置为当前最大事件时间戳 - 1毫秒
            return new AscendingTimestampsWatermarks<>();
        }
    
        /**
         * 创建时间戳分配器
         * 负责从事件中提取或生成事件时间戳
         * 
         * @param context 时间戳分配器上下文
         * @return 返回一个Lambda表达式作为时间戳分配器
         *         这里使用系统当前时间作为事件时间戳，而不是从事件内容中提取
         */
        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            // 使用系统当前时间作为事件时间戳
            // event: 当前处理的事件对象
            // timestamp: 之前分配的时间戳，如果有的话
            // 返回值: 分配给事件的新时间戳（毫秒时间戳）
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}