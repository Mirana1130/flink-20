package day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//TODO 流处理
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境 用StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.读取文本数据 hello mirana flink
        DataStreamSource<String> input = env.readTextFile("input");
        env.setParallelism(1); //为了方便显示结果数据的生成过程
        //TODO 3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordone = input.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        //TODO 4.分组 没有groupBy,用KeyBy KeyBy规约了传输原则,按照key的hash值传输到分区
                            //sum方法求和时，每个Key有个状态，不是每个分区/并行度有个状态
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordone.keyBy(0);
        //TODO 5.计算WordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //TODO 6.打印
        result.print();
        //TODO 7.启动任务 流任务需要启动
        // 来一条处理一条，打印一条
        env.execute("Flink02_WordCount_Bounded");
    }
}
