package top.mangod.flinkblog.demo007iotdb;

import org.apache.flink.table.api.*;

/**
 * IoTDB CDC（Change Data Capture）测试类
 * 该类演示如何使用Flink Table API连接IoTDB数据库并以CDC模式读取变更数据
 */
public class CDCTest {
    /**
     * 主方法，程序入口
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        // 配置Flink执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()  // 创建环境设置实例
                .inStreamingMode()  // 设置为流处理模式
                .build();  // 构建环境设置
        
        // 创建TableEnvironment，这是Flink Table API的核心入口
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        // 定义表结构(Schema)
        Schema iotdbTableSchema = Schema
                .newBuilder()  // 创建Schema构建器
                .column("Time_", DataTypes.BIGINT())  // 定义时间戳字段，类型为BIGINT
                .column("root.sg.d0.s0", DataTypes.FLOAT())  // 定义IoT设备路径和对应的数据类型
                .column("root.sg.d1.s0", DataTypes.FLOAT())  // 定义另一个IoT设备路径和对应的数据类型
                .column("root.sg.d1.s1", DataTypes.FLOAT())  // 定义第三个IoT设备路径和对应的数据类型
                .build();  // 构建Schema

        // 创建TableDescriptor来描述如何连接和读取IoTDB
        TableDescriptor iotdbDescriptor = TableDescriptor
                .forConnector("IoTDB")  // 指定使用IoTDB连接器
                .schema(iotdbTableSchema)  // 应用之前定义的Schema
                .option("mode", "CDC")  // 设置模式为CDC（变更数据捕获）
                .option("cdc.task.name", "test")  // 设置CDC任务名称
                .option("cdc.pattern", "root.sg")  // 设置CDC监听的路径模式
                .build();  // 构建TableDescriptor
                
        // 注册临时表，将其命名为"iotdbTable"
        tableEnv.createTemporaryTable("iotdbTable", iotdbDescriptor);

        // 从临时表中读取数据并执行打印操作
        // from()方法创建一个Table对象，表示从iotdbTable读取数据
        // execute()提交执行作业
        // print()将结果打印到控制台
        tableEnv.from("iotdbTable").execute().print();
    }
}