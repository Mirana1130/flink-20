package day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @author Mirana
 */
public class Flink02_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件读取数据创建流
        DataStreamSource<String> socketDStream = env.socketTextStream("hadoop102", 9999);
        socketDStream.addSink(new MyJdbcSinkfunc());
        env.execute();
    }
    public static class MyJdbcSinkfunc extends RichSinkFunction<String>{
       private Connection conn;
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            // open 主要是创建连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            // 创建预编译器，有占位符，可传入参数
            preparedStatement = conn.prepareStatement("INSERT INTO sensor_id(id,temp) VALUE(?,?) ON DUPLICATE KEY UPDATE temp=?;");
        }
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            conn.close();
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
         //切割
            String[] fields = value.split(",");
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setString(2,fields[2]);
            preparedStatement.setString(3,fields[2]);
            preparedStatement.execute();
        }
    }
}