package day05;
import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Flink01_State_Ontimer_TempNotDesc {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.1读取端口数据,转换为JavaBean
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> sensorDs = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
//        //2.2读取自定义source数据,转换为JavaBean
//        DataStreamSource<SensorReading> sensorDs = env.addSource(new MySource());
        //3.分组(按SensorReading的Id属性)
        KeyedStream<SensorReading, String> keyedStream = sensorDs.keyBy(SensorReading::getId);
        //4.使用ProcessAPI实现10秒温度没有下降则报警逻辑
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyProcessFunc(10));
        result.print();
        env.execute();
    }
    public static class MyKeyProcessFunc extends KeyedProcessFunction<String,SensorReading,String>{
        //定义属性,时间间隔
        private Integer inteval;
        //声明状态用于存储每一次的温度值
        private ValueState<Double> tempState;
        //声明状态用于存放定时器时间
        private ValueState<Long> tsState;

        //添加构造器让业务更通用->可以传参监控多少秒的报警逻辑
        public MyKeyProcessFunc(Integer inteval){
            this.inteval=inteval;
        }

        //TODO 从生命周期获取状态 open()方法
        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-State", Double.class,Double.MIN_VALUE));
            tsState= getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-State", Long.class));
        }
        //TODO 逻辑代码 processElement()方法
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //取值and更新值，方便下面调用
            Double lastTemp = tempState.value();
            Long ts = tsState.value();
            Double curTemp = value.getTemp();
            tempState.update(curTemp);
            //  => 需要注册定时器的情况           1)条件与2) &&关系
            // 1)如果当前的温度 > 上一次的温度   (根据业务逻辑，温度升高注册才有意义)
            // 2)定时器的值为null               (第一次的情况和状态清空的情况，)
            if (curTemp>lastTemp && ts ==null){
                long curTs = ctx.timerService().currentProcessingTime()+inteval*1000L;
                ctx.timerService().registerProcessingTimeTimer(curTs);
                tsState.update(curTs);
            }
            //  => 需要清空定时器的情况           1)条件与2) &&关系
            // 1)如果当前的温度 < 上一次的温度   (根据业务逻辑，10秒内温度下降无需报警)
            // 2)定时器的值不为空               (为空的话不需要清空)
            else if (value.getTemp()<lastTemp && ts !=null){
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    tsState.clear();
            }
        }
        //TODO  触发定时器任务 onTimer()方法     触发后清空定时器，不清空的话当温度下降时多走上面一层
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+"连续10s温度没有下降！");
            tsState.clear();
        }
    }
    //mycode 自定义Source
    public static class MySource implements SourceFunction<SensorReading> {
        //1.1 定义一个标记任务的运行和关闭
        private Boolean running=true;
        //1.2 定义一个随机数
        private Random random=new Random();
        //1.3 定义SensorReading基准值
        private Map<String,SensorReading> map=new HashMap<>();
        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //1.4 给各个传感器赋值基准值
            //1.4.1 5个传感器id:1-5
            for (int i = 0; i <5 ; i++) {
                String id = "sensor_" + (i + 1);
                map.put(id,new SensorReading(id,System.currentTimeMillis(),60D+random.nextGaussian()*20));
            }
            while (running){
                //1.4.2 遍历map
                for (Map.Entry<String, SensorReading> entry : map.entrySet()) {
                    String id = entry.getKey();
                    SensorReading value = entry.getValue();
                    Double temp = value.getTemp();
                    sourceContext.collect(new SensorReading(id,System.currentTimeMillis(),temp+random.nextGaussian()));
                }
                Thread.sleep(5000);
            }
        }
        @Override
        public void cancel() {
            running=false;
        }
    }
}
