package day02;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.Set;

public class Flink20_Transform_work04 {
    public static void main(String[] args) throws Exception {
        //TODO 读取文本数据,拆分成单个单词,对单词进行去重输出(即某个单词被写出过一次,以后就不再写出)。
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOne.filter(new MyRichFilter());
        result.print();
        env.execute();
    }
    public static class MyRichFilter extends RichFilterFunction<Tuple2<String, Integer>>{
      private Jedis jedis;
      private String jediskey="words";
        @Override
        public void open(Configuration parameters) throws Exception {
            jedis=new Jedis("hadoop102",6379);
        }
        @Override
        public void close() throws Exception {
            jedis.close();
        }

        @Override
        public boolean filter(Tuple2<String, Integer> value) throws Exception {
            Set<String> set = jedis.smembers(jediskey);
            boolean contains = set.contains(value);
            if (!contains){
                jedis.sadd(jediskey,value.f0);
                return true;
            }else {
                return false;
            }
        }
    }
}

//my未
//        env.setParallelism(1);
//                DataStreamSource<String> textDStream = env.readTextFile("input");
//        SingleOutputStreamOperator<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> wordToone = textDStream.flatMap(new FlatMapFunction<String, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
//@Override
//public void flatMap(String value, Collector<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> out) throws Exception {
//        String[] words = value.split(" ");
//        for (String word : words) {
//        out.collect(new org.apache.flink.api.java.tuple.Tuple2<>(word, 1));
//        }
//        }
//        });
//        //按照word进行分组
//        KeyedStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>, Tuple> keyedStream = wordToone.keyBy(0);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//@Override
//public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//        return value1;
//        }
//        });
//        result.print();