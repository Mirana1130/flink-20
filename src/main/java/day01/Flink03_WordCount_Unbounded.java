package day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//TODO 无界流处理
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取端口数据
        //传参数的方式
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
//        DataStreamSource<String> socketTextStream = env.socketTextStream(host, port);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //TODO 3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        //TODO 4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //TODO 5.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //TODO 6.打印
        keyedStream.print("keyedStream:");
        result.print("result:");
        //TODO 7.启动任务
        env.execute();
    }
}