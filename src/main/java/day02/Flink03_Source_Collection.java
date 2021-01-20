package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author Mirana
 */
public class Flink03_Source_Collection {
    public static void main(String[] args) throws Exception {
        ////1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // //2.从集合获取数据创建流
        List<SensorReading> sensorReadings = Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        );
        DataStreamSource<SensorReading> sensorDataStream  = env.fromCollection(sensorReadings);
        sensorDataStream.print();
        env.execute();
    }
}
