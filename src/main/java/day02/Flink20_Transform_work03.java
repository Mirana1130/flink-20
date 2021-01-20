package day02;


import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.HashSet;

//TODO 读取文本数据,拆分成单个单词,对单词进行去重输出(即某个单词被写出过一次,以后就不再写出)。
public class Flink20_Transform_work03 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HashSet<String> set = new HashSet<>();
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordToOne.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                boolean contains = set.contains(value.f0);
                if (!contains) {
                    set.add(value.f0);//如果不包含写入set,供下一个数据和set里数据去重
                    return true;//不包含表示这个词未出现过 返回true,不过滤 反之过滤
                }
                {
                    return false;
                }
            }
        });
        result.print();
        env.execute();
    }
}