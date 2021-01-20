package day04;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink12_State_TempDiff {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3.转换未javabean，并分组
        KeyedStream<SensorReading, Tuple> keyedStream = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).keyBy("id");
        //TODO 4.使用RichFunction 实现状态编程，如果同一个传感器连续两次温度差值超过10度，则输出报警信息
        SingleOutputStreamOperator<String> result = keyedStream.map(new myRichMapFunction(30.0));
        //5.打印
        result.print();
        //6.执行
        env.execute();
    }
    public static class myRichMapFunction extends RichMapFunction<SensorReading,String>{
        private Double maxDiff;
        private ValueState<Double> tempState;

        public myRichMapFunction(Double maxDiff) {
            this.maxDiff = maxDiff;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
        }
        @Override
        public String map(SensorReading value) throws Exception {
            Double lasttemp = tempState.value();
            tempState.update(value.getTemp());
            if (lasttemp !=null && Math.abs(lasttemp-value.getTemp())>maxDiff){
                return "连续两次温度差值超过" + maxDiff + "度";
            }
                return "";
        }
    }
}