package day03;

import day01.Flink01_WordCount_Batch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Count;

import java.util.Iterator;

//滚动事件窗口，计算最近10秒的数据wordcount
public class Flink08_Window_TimeTumbling_apply {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件读取数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlatMapFunc());
        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //5.TODO 开10秒的窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(new MyWindowFunc());
        //6.TODO 计算10秒内的单词出现次数
        //7.打印
        result.print();
        env.execute();
    }
    public static class MyWindowFunc implements WindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>,Tuple,TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取key值 （分组后的单词 words）
            String key = tuple.getField(0);
            int count =0;
            //获取words的个数
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            while (iterator.hasNext()){
//                input.iterator().next();
//                count+=1;
                count+=iterator.next().f1;
            }
            out.collect(new Tuple2<>(key,count));
        }
    }
}
