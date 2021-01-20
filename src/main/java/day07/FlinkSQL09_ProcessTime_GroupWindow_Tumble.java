package day07;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
/**
 * @author Mirana
 */
public class FlinkSQL09_ProcessTime_GroupWindow_Tumble {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });
        //3.TODO 将流转换为表并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");
        //4.TODO 基于时间的滚动窗口TableAPI   窗口事件别名 tw  按tw,id分组
        Table tableResult = table.window(Tumble.over("10.seconds").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count,tw.start");
        //5.TODO 基于时间的滚动窗口SQL API
                tableEnv.createTemporaryView("sensor", table);
                Table sqlResult = tableEnv.sqlQuery("select id,count(id),tumble_end(pt,interval '10' second) " +
                        "from sensor " +
                        "group by id,tumble(pt,interval '10' second)");
        //6.转换为流进行输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
        //7.执行任务
        env.execute();
    }
}

//sensor_1,1547718199,35.8  (间隔一会)
//sensor_5,1547718201,15.4  (间隔一会)
//sensor_7,1547718202,6.7   (间隔一会)
//sensor_5,1547718201,15.4  连续
//sensor_5,1547718201,15.4
//(间隔一会)
//sensor_1,1547718199,35.8
//sensor_1,1547718199,35.8
//sensor_1,1547718199,35.8

//打印
//SQL> sensor_1,1,2020-12-18 04:47:10.0
//Table> sensor_1,1,2020-12-18 04:47:00.0

//Table> sensor_5,1,2020-12-18 04:48:10.0
//SQL> sensor_5,1,2020-12-18 04:48:20.0

//Table> sensor_7,1,2020-12-18 04:48:50.0
//SQL> sensor_7,1,2020-12-18 04:49:00.0

//SQL> sensor_5,2,2020-12-18 04:49:10.0
//Table> sensor_5,2,2020-12-18 04:49:00.0

//SQL> sensor_1,3,2020-12-18 04:49:30.0
//Table> sensor_1,3,2020-12-18 04:49:20.0