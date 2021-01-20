package day07;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author Mirana
 */
public class FlinkSQL07_EventTime_DataSource {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 设置事件事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建文件连接器 TODO 这里指定
        tableEnv.connect(new FileSystem().path("sensor"))
                .withSchema(new Schema()
                            .field("id", DataTypes.STRING())
                            .field("ts", DataTypes.BIGINT())
                            .rowtime(new Rowtime()
                                    .timestampsFromField("ts")    // 从字段中提取时间戳
                                    .watermarksPeriodicBounded(1000)    // watermark延迟1秒
                                    )
                            .field("temp", DataTypes.DOUBLE())
                            )
                .withFormat(new Csv())
                .createTemporaryTable("fileInput");

        //3.打印schema信息
        Table table = tableEnv.from("fileInput");
        table.printSchema();


    }
}
