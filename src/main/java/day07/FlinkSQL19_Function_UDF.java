package day07;

import bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class FlinkSQL19_Function_UDF {

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
        //3.将流转换为表并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS);
        //4.TODO 注册函数
        tableEnv.registerFunction("mylen",new MyLength());
        //4.TODO TableAPI
        Table tableresult = table.select("id,id.mylen");
        //5.TODO SQLAPI
        Table sqlResult = tableEnv.sqlQuery("select id,mylen(id) from " + table);
        //6.转换为流进行打印输出
        tableEnv.toAppendStream(tableresult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
        //7.执行
        env.execute();
    }
    //TODO 自定义标量函数 注意eval()
    public static class MyLength extends ScalarFunction{
        public int eval(String value){
            return value.length();
        }
    }
}