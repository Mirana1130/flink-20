package day04;

import com.sun.prism.impl.paint.PaintUtil;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 TODO 未KeyedProcessFunction（在keyedStream后使用） 完成高温低温数据分开到两个流
 */
public class Flink10_ProcessAPI_SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        KeyedStream<String, String> keyedStream = socketStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        SingleOutputStreamOperator<String> result = keyedStream.process(new MySideOutPutProcessFunc());
        result.print("high");
        result.getSideOutput(new OutputTag<Tuple2<String,Double>>("sideOutPut"){}).print("low");
        env.execute();
    }
   public static class MySideOutPutProcessFunc extends KeyedProcessFunction<String,String,String>{
       @Override
       public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //切割数据，获取其中对的温度值
           String[] fields = value.split(",");
           double temp = Double.parseDouble(fields[2]);
           if (temp >30.0){
               //高温数据，输出到主流
               out.collect(value);
           }else {
               ctx.output(new OutputTag<Tuple2<String,Double>>("sideOutPut"){},new Tuple2<>(fields[0],temp));
           }
       }
   }
}
