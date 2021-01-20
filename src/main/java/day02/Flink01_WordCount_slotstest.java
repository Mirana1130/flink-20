package day02;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_WordCount_slotstest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //TODO source
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //TODO flatMap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc())
                .slotSharingGroup("group1");

        //TODO keyBy
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);

        //TODO sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1).slotSharingGroup("group2");

        //TODO print
        result.print("result:");

        env.execute();
    }
}
