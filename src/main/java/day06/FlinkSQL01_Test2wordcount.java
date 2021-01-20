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
public class FlinkSQL01_Test2wordcount {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境 为了方便查看结果设置并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2.读取端口数据转换为javabean
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> sensorStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        // 3.将流进行注册，注册为表
        tableEnv.createTemporaryView("sensor",sensorStream);

        //TODO 4.SQL方式实现查询(查询id=1的id信息和temp信息)
        Table sqlresult = tableEnv.sqlQuery("select id,count(id) from sensor group by id");

        //TODO 5.TableAPI
        Table table = tableEnv.fromDataStream(sensorStream);
        Table tableresult = table.groupBy("id").select("id,id.count");

        //TODO 6.将表转换为流进行输出
        //因为数据来了以后结果是(sensor_1,1)->(sensor_1,2)->(sensor_1,2)
        // 如果使用toAppendStream，会报错Table is not an append-only table，需求结果不是要一条追加一条的，结果需要覆盖
        // TODO 所以使用撤回流 toRetractStream 将(sensor_1,1)撤回 将新结果输出
        tableEnv.toRetractStream(sqlresult, Row.class).print("sql");
        tableEnv.toRetractStream(tableresult, Row.class).print("table");

        //执行
        env.execute();
    }
}
//table> (true,sensor_1,1)
//sql> (true,sensor_1,1)

//sql> (true,sensor_7,1)
//table> (true,sensor_7,1)

//sql> (false,sensor_7,1)
//sql> (true,sensor_7,2)
//table> (false,sensor_7,1)
//table> (true,sensor_7,2)