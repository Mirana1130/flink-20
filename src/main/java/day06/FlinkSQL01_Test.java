package day06;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Mirana
 */
public class FlinkSQL01_Test {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境 为了方便查看结果设置并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 获取TableAPI的执行环境（类似于spark session调用sql方法）
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.读取端口数据转换为javabean
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //TODO 3.将流进行注册，注册为表
        tableEnv.createTemporaryView("sensor",sensorStream);

        //TODO 4.SQL方式实现查询(查询id=1的id信息和temp信息)
        Table sqlresult = tableEnv.sqlQuery("select id,temp from sensor where id='sensor_1'");

        //TODO 5.TableAPI
        Table table = tableEnv.fromDataStream(sensorStream);
        Table tableresult = table.select("id,temp").where("id = 'sensor_1'");

        //TODO 6.将表转换为流进行输出 toAppendStream Row 打印
        tableEnv.toAppendStream(sqlresult, Row.class).print("sql");
        tableEnv.toAppendStream(tableresult, Row.class).print("table");

        //执行
        env.execute();
    }
}
