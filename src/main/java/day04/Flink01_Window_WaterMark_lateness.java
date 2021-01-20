package day04;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//TODO 滚动事件窗口，5秒的滚动窗口。waterma2秒，允许延迟2秒+测输出流
//  例如：sensor_1,8199,35 ->8199->[8195,8200)内，
//  是增量聚合运算sum，来一条计算一条
//  第一次输出是watermark到窗口结束时间的时候->watermark(8200),数据（8202）
//  在窗口结束时间（8202）到允许迟到时间（8204）区间内的数据来一条计算一条
//  到了（8204）以后，后面的迟到数据放到测输出流

//TODO 同样要处理网络延迟30秒的情况，
//  waterma 设置35秒需要维护很多窗口，数据输出延迟，效率差
//  允许延迟2秒+测输出流 先让窗口关闭，让特别延迟的数据放在测输出流，后面再处理 效率更高
public class Flink01_Window_WaterMark_lateness {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 引入事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.从文件读取数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //TODO 指定事件时间字段AssignerWithPeriodicWatermarks和watermark时间
        SingleOutputStreamOperator<String> OutputStreamOperator = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });
        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = OutputStreamOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0],1);
            }
        });
        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //5.TODO 开5秒的窗口 + 允许延迟2秒+测输出流
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> sideOutPut = keyedStream
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});
        //6.TODO 计算5秒内的单词出现次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sideOutPut.sum(1);
        //7.打印
        result.print("result");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){}).print("sideOutPut");
        env.execute();
    }
}
