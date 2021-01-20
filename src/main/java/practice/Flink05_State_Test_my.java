package practice;
import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class Flink05_State_Test_my {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取kafka数据,转换为JavaBean
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"Bigdata0720");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");//注意不要写错
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<SensorReading> sensorDs = kafkaStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        //3.分组(按SensorReading的Id属性)
        KeyedStream<SensorReading, String> keyedStream = sensorDs.keyBy(SensorReading::getId);
        //4.使用ProcessAPI实现10秒温度没有下降则报警逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultA = keyedStream.process(new MyKeyProcessFunc(10));

        DataStream<Tuple2<String, String>> sideOutput = resultA.getSideOutput(new OutputTag<Tuple2<String, String>>("side") {
        });

        SingleOutputStreamOperator<String> mapA = resultA.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + "," + value.f1;
            }
        });
        mapA.addSink(new MyJdbcSinkfunc("sensor_A"));
        mapA.print("sensor_A");

        SingleOutputStreamOperator<String> mapB = sideOutput.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> value) throws Exception {
                return value.f0 + "," + value.f1;
            }
        });
        mapB.addSink(new MyJdbcSinkfunc("sensor_B"));
        mapB.print("sensor_B");
        env.execute();
    }
    public static class MyKeyProcessFunc extends KeyedProcessFunction<String,SensorReading,Tuple2<String,Integer>>{
        //TODO 定义属性,次数
        private ValueState<Integer> countState;
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
            countState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Count-State", Integer.class,0));
        }
        //TODO 逻辑代码 processElement()方法
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
            //取值and更新值，方便下面调用
            Integer count = countState.value();

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
            out.collect(new Tuple2<>(value.getId(), count + 1));
            countState.update(count+1);

        }
        //TODO  触发定时器任务 onTimer()方法     触发后清空定时器，不清空的话当温度下降时多走上面一层
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String,Integer>> out) throws Exception {
            ctx.output(new OutputTag<Tuple2<String,String>>("side"){},new Tuple2<>(ctx.getCurrentKey(),"tempbuxiajiang！"));
            tsState.clear();
        }
    }
    public static class MyJdbcSinkfunc extends RichSinkFunction<String> {
        private Connection conn;
        private PreparedStatement preparedStatement;
        private String tablename;

        public MyJdbcSinkfunc(String tablename) {
            this.tablename=tablename;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // open 主要是创建连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root", "123456");
            // 创建预编译器，有占位符，可传入参数
           // preparedStatement = conn.prepareStatement("INSERT INTO sensor_id(id,temp) VALUE(?,?) ON DUPLICATE KEY UPDATE temp=?;");
            preparedStatement = conn.prepareStatement("INSERT INTO "+tablename+" (id,result) VALUE(?,?) ON DUPLICATE KEY UPDATE result=?;");
        }
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            conn.close();
        }
        @Override
        public void invoke(String value, Context context) throws Exception {
            //切割
            String[] fields = value.split(",");
            preparedStatement.setString(1,fields[0]);
            preparedStatement.setString(2,fields[1]);
            preparedStatement.setString(3,fields[1]);
            preparedStatement.execute();
        }
    }
}
