package day03;

import bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 Flink中窗口操作的分类,编写代码从端口获取数据实现每隔5秒钟计算最近30秒内每个传感器的最高温度(使用事件时间).
 */
public class Flink10_work2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 引入事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //TODO 指定事件时间字段 设定水位线的乱序时间 2s
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });
        //转换为javabean
        SingleOutputStreamOperator<SensorReading> sensorReadingDS = stringSingleOutputStreamOperator.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        KeyedStream<SensorReading, Tuple> keyedStream = sensorReadingDS.keyBy("id");
        WindowedStream<SensorReading, Tuple, TimeWindow> sensortimewindow = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5));
        SingleOutputStreamOperator<SensorReading> result = sensortimewindow.max("temp");
        result.print();
        env.execute();
    }
}
