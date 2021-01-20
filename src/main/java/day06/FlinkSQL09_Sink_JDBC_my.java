package day06;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSQL09_Sink_JDBC_my {

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

        //3.对流进行注册
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.TableAPI
        Table tableResult = table.groupBy("id").select("id,id.count");

        //5.SQL
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,count(*) as ct from sensor group by id");

        //6.创建连接器 JDBC DDL
//        String jdbcDDL="create table jdbcOutputTable (" +
//                "id varchar(20) not null," +
//                "ct bigint not null) with (" +
//                " 'connector.type' = 'jdbc'," +
//                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test'," +
//                "'connector.table' = 'sensor_count'," +
//                " 'connector.driver' = 'com.mysql.jdbc.Driver'," +
//                " 'connector.username' = 'root'," +
//                "  'connector.password' = '123456')";
        String jdbcDDL = "create table jdbcOutputTable (" +
                " id varchar(20) not null, " +
                " ct bigint not null " +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                " 'connector.table' = 'sensor_count', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                " 'connector.username' = 'root', " +
                " 'connector.password' = '123456' ," +
                " 'connector.write.flush.interval' = '2s' )";

        tableEnv.sqlUpdate(jdbcDDL);
        //7.将数据写入文件系统
        sqlResult.insertInto("jdbcOutputTable");
       // tableEnv.insertInto("jdbcOutputTable", sqlResult);

        //8.执行
        env.execute();
    }
}