package top.mangod.flinkblog.demo004;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author: baily
 * @mail: yclxiao@gmail.com
 **/
public class WatermarkSocketWindowWordCount {
    /**
     * 主方法 - 实现基于事件时间和水印的Flink流处理示例
     * 演示如何使用水印机制处理乱序事件数据，并进行窗口计算
     * @param args 命令行参数
     * @throws Exception 执行过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流处理执行环境
        // 这是所有Flink流处理程序的起点，负责管理作业的执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
        // 2. 定义数据源
        // 从本地9999端口读取Socket文本流，以换行符("\n")作为数据分隔符
        DataStream<String> textStream = env.socketTextStream("localhost", 9999, "\n");
    
        // 3. 数据转换与处理流程
        // 输入数据格式示例：1000,a   2000,a  3000,b （时间戳，单词）
        DataStream<Tuple2<String, Integer>> windowCountStream = textStream
                // 4. 配置水印策略
                // forBoundedOutOfOrderness：设置允许的最大乱序时间为2秒
                // 即允许事件延迟最多2秒到达，超过这个时间的数据将被视为迟到数据
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                // 从输入事件中提取时间戳
                                // 将每行数据按逗号分割，取第一个字段作为事件时间戳
                                .withTimestampAssigner((event, timestamp) ->
                                        Long.parseLong(event.split(",")[0])))
            
            // 5. 数据格式转换
            // 将输入的字符串转换为Tuple2<单词, 1>格式
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    // 按逗号分割输入数据
                    String[] splits = value.split(",");
                    // 返回单词(第二个字段)和计数1
                    return Tuple2.of(splits[1], 1);
                }
            })
            
            // 6. 按单词进行分组
            // 根据元组的第一个字段(单词)进行keyBy操作
            .keyBy(value -> value.f0)
            
            // 7. 设置滚动事件时间窗口
            // 创建长度为5秒的滚动窗口(注释写的是5分钟，但实际代码是5秒)
            // 注意：这是基于事件时间的窗口，不是处理时间
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            
            // 8. 执行聚合计算
            // 对窗口内的数据，按元组的第二个字段(计数)进行求和
            .sum(1);
    
        // 9. 结果输出
        // 打印计算结果到控制台，前缀为" ========== "，并行度设为1确保输出顺序
        windowCountStream.print(" ========== ").setParallelism(1);
    
        // 10. 启动Flink作业
        // 以类名作为作业名称执行Flink程序
        env.execute(WatermarkSocketWindowWordCount.class.getSimpleName());
    }
}