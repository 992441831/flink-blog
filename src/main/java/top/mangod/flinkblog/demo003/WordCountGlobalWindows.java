package top.mangod.flinkblog.demo003;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 * @created: 2023/7/10 16:07
 * 本文DEMO示例：每5分钟统计一次单词的数量
 **/
public class WordCountGlobalWindows {
    /**
     * 基于Flink的流处理单词计数应用
     * 使用全局窗口(GlobalWindows)和自定义触发器实现的单词统计
     * 适用于需要自定义触发逻辑和数据清理的场景
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
                
                // 使用全局窗口
                // GlobalWindows.create()创建一个包含所有相同key数据的全局窗口
                .window(GlobalWindows.create())
                
                // 设置清除器(Evictor)，用于清除超过指定时间的数据
                // 这里设置30秒的过期时间，防止整个窗口数据量过大导致内存溢出
                .evictor(TimeEvictor.of(Time.of(30, TimeUnit.SECONDS)))
                
                // 设置自定义触发器
                // 使用WindowWordCount02内部的MyTrigger类作为触发器
                // 传入序列化器参数以支持分布式环境下的数据序列化
                .trigger(new WordCountGlobalWindows.MyTrigger(textStream.getType().createSerializer(env.getConfig())))
                
                // 对每个组内的单词计数进行滚动相加
                // 使用ReduceFunction将相同单词的计数累加
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
                        // 返回累加后的结果：保留单词(a.f0)，累加计数值(a.f1 + b.f1)
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });
    
        // 4. 数据输出
        // 将单词计数结果打印到控制台，并设置并行度为1确保输出顺序性
        wordCountStream.print("WindowWordCount02 ======= ").setParallelism(1);
        
        // 5. 启动任务
        // 执行Flink作业，以类名作为作业名称
        env.execute(WordCountGlobalWindows.class.getSimpleName());
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

    private static class MyTrigger<T, W extends Window> extends Trigger<T, W> {
        private final ValueStateDescriptor<T> stateDesc;

        private MyTrigger(TypeSerializer<T> stateSerializer) {
            stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);
        }

        @Override
        public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            Tuple2<String, Integer> elementValue = (Tuple2<String, Integer>) element;
            ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
            if (lastElementState.value() == null) {
                lastElementState.update(element);
                return TriggerResult.CONTINUE;
            }
            // 此处状态描述器ValueState可以不使用
            Tuple2<String, Integer> lastValue = (Tuple2<String, Integer>) lastElementState.value();
            if (elementValue.f0.equals("1")) {
                lastElementState.update(element);
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
        }
    }
}