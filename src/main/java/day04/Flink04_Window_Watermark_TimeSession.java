package day04;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//TODO 会话窗口开5秒，watermark开2秒
//  例如：1.当第一条数据 sensor_1,1547718199,35.8来时
//  不打印
//        2.当sensor_1,1547718206,23 来时打印（sensor_1,1547718199,35.8，1）->当watermark比数据延迟5秒即数据于数据相差7秒才会输出
//        3.当sensor_1,1547718211,63
//  不打印
//        4.当sensor_1,1547718213,56 来时打印 (sensor_1,1547718206,23,1) -> (1547718213 -1547718206)=7
//        4.如果不来数据sensor_1,1547718213，来的直接是sensor_1,1547718218,56 打印 (sensor_1,1547718206,23,1)和 (sensor_1,1547718211,63，1）两条数据的窗口都关闭且输出

public class Flink04_Window_Watermark_TimeSession {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置分区便于看结果
        env.setParallelism(1);
        //TODO 2.设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.读取端口数据创建流
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("hadoop102", 9999).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });
        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //TODO 5.开会话窗口 5秒 需要手动修改EventTimeSessionWindows
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        //6.聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);
        //7.打印
        result.print();
        //8.执行
        env.execute();
    }
}