package day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import static java.lang.Runtime.getRuntime;

public class Flink11_State_ProcessAPI_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 一般来说单独配置一个文件避免权限问题，如果在配置文件里是全局的路径，访问和操作不一样有权限
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink"));
        env.enableCheckpointing(5000L);
        //如果设置不删除的话可以在网页提交时cancel情况下不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3.TODO 压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.process(new MyFlatMapProcessFunc());
        //4.TODO 将每个单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.process(new MyMapProcessFunc());
        //5.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);
        //6.TODO 聚合数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new MySumKeyedProcessFunc());
        //7.打印
        result.print();
        //8.执行
        env.execute();
    }
    public static class MyFlatMapProcessFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //切割
            String[] words = value.split(" ");
            //遍历输出
            for (String word : words) {
                out.collect(word);
            }
        }
    }
    public static class MyMapProcessFunc extends ProcessFunction<String, Tuple2<String, Integer>> {
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value, 1));
        }
    }
    public static class MySumKeyedProcessFunc extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        //在这个位置会报错,原因在于,类加载的时候还没上下文环境
        //ValueState<Integer> countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Count-State", Integer.class));
        ValueState<Integer> state;
        @Override
        public void open(Configuration parameters) throws Exception {
           state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Count-State", Integer.class,0));
          //state.update(0);如果是用这种方式初始化值，会报错没有指定key，
            // 我们创建的ValueState是一种key的State（键控状态），每一种key有一个状态
            //open方法，一个并行度只有一个，一个并行度下有多种key,没有指定key。
        }
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer count = state.value();
            out.collect(new Tuple2<>(value.f0, count + 1));
            state.update(count+1);
        }
    }
}