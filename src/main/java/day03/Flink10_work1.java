package day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 TODO 读取Kafka主题的数据计算WordCount并存入MySQL.
 */
public class Flink10_work1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"mirana1214");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource kafkasensorDStream = env.addSource(new FlinkKafkaConsumer011("sensor_work", new SimpleStringSchema(), properties));
        kafkasensorDStream.addSink(new MyJDBCSinkFunc());
        env.execute();
    }
    public static class MyJDBCSinkFunc extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            preparedStatement = connection.prepareStatement("INSERT INTO sensor_id(id,temp) VALUE(?,?) ON DUPLICATE KEY UPDATE temp=?;");
        }
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] fields = value.split(",");
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setString(2,fields[2]);
            preparedStatement.setString(3,fields[2]);
            preparedStatement.execute();
        }
    }
}
