package day06;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Mirana
 */
public class FlinkSQL02_Env {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//TODO 1.老版本
        // TODO 1.1流式处理环境
        //------------------------------------------------------------------------------
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()      // TODO 使用老版本planner
                .inStreamingMode()    // TODO 流处理模式
                .build();
        //------------------------------------------------------------------------------
        //我们之前创建的省略了前面的步骤 直接是 StreamTableEnvironment.create(env)
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 1.2老版批式处理环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

//TODO 新版本
        //TODO 2.1 流式处理环境 (Blink 1.11版本会自动使用 1.10的话需要手动这样设置)
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
        //2.2老版批式处理环境
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
    }
}
