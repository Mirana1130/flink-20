package day06;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author Mirana
 */
public class FlinkSQL06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.从端口读数据 转换为javabean
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        //3.转化为表 查询
        Table table = tableEnv.fromDataStream(sensorStream);
        Table tableresult = table.select("id,temp");

        tableEnv.createTemporaryView("sensor",sensorStream);
        Table sqlresult = tableEnv.sqlQuery("select id,temp from sensor");
        //4.连接器
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
        )
                //.withFormat(new Csv())
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("kafkaOut");
        tableEnv.insertInto("kafkaOut",tableresult);
        tableEnv.insertInto("kafkaOut",sqlresult);

        env.execute();
    }
}
