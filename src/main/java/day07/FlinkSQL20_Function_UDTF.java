package day07;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class FlinkSQL20_Function_UDTF {

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
        tableEnv.registerFunction("split",new split());
        //4.TODO TableAPI
        Table tableresult = table.joinLateral("split(id) as (word,len)").select("id,word,len");
        //5.TODO SQLAPI
        Table sqlResult = tableEnv.sqlQuery("select id,word,len from " + table +",lateral table(split(id)) as splitTable(word,len)");
        //6.转换为流进行打印输出
        tableEnv.toAppendStream(tableresult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
        //7.执行
        env.execute();
    }
    //TODO 自定义UDTF函数 一进多出  <T>里填的是列的类型，输出的是多行数据，和flatmap类似
    //  eval()方法 可调用collect写出
    //计算 id字符串 (sensor_1) 按下划线炸开后单词和单词的长度 sensor,6 1,1
    public static class split extends TableFunction<Tuple2<String,Integer>> {
        public void eval(String value){
            String[] fields = value.split("_");
            for (String field : fields) {
                collect(new Tuple2<>(field,field.length()));
            }
        }
    }
}
//TODO table为例
//[nc -lk 9999] sensor_1,1547718199,35.8
//[nc -lk 9999] sensor_1_2,156885522,30
//------------------------ sensor_1----------------------------
//Table> sensor_1,sensor,6
//Table> sensor_1,1,1
//------------------------ sensor_1_2----------------------------
//Table> sensor_1_2,sensor,6
//Table> sensor_1_2,1,1
//Table> sensor_1_2,2,1
