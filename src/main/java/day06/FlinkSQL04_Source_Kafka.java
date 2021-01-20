package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * TODO 从文件系统读取文件 打印到控制台
 */
public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO  2. Kafka Connector (Source: Streaming Append Mode Sink: Streaming Append Mode ; Format: CSV, JSON, Avro)
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("test")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "mirana1216")
                        )
                //.withFormat(new Csv())
                .withFormat(new Json())
                .withSchema(new Schema()
                            .field("id",DataTypes.STRING())
                            .field("ts",DataTypes.BIGINT())
                            .field("temp",DataTypes.DOUBLE())
                            )
                .createTemporaryTable("kafka");
        Table kafkatable = tableEnv.from("kafka");
        Table sqlresult = tableEnv.sqlQuery("select id,min(temp) from kafka group by id");
        Table tableresult = kafkatable.groupBy("id").select("id,temp.max");

        //打印  撤回流
        tableEnv.toRetractStream(sqlresult, Row.class).print("sql");
        tableEnv.toRetractStream(tableresult, Row.class).print("table");

        env.execute();
    }
}
//kafka Csv格式
    //[atguigu@hadoop102 kafka]$ bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic test
    //>sensor_1,1547718199,35.8
    //>sensor_1,1547718199,10.0
    //>sensor_1,1547718199,40.0
//打印结果
    //sql> (true,sensor_1,35.8)
    //table> (true,sensor_1,35.8)
    //sql> (false,sensor_1,35.8)
    //sql> (true,sensor_1,10.0)
    //table> (false,sensor_1,35.8)
    //table> (true,sensor_1,40.0)

//kafka json格式
    //[atguigu@hadoop102 kafka]$ bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic test
    //>{"id":"sensor_1","ts":"1547718199","temp":"35.8"}
//打印结果
    //sql> (true,sensor_1,35.8)
    //table> (true,sensor_1,35.8)