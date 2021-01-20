package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import sun.management.counter.Variability;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * @author Mirana
 */
public class Flink07_Source_Customer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取数据
        DataStreamSource<SensorReading> mysourceDStream = env.addSource(new MySource());
        mysourceDStream.print();
        env.execute();
    }//TODO 1.自定义一个SourceFunction
    public static class MySource implements SourceFunction<SensorReading>{
        //1.1 定义一个标记任务的运行和关闭
        private Boolean running=true;
        //1.2 定义一个随机数
        private Random random=new Random();
        //1.3 定义SensorReading基准值
        private Map<String,SensorReading> map=new HashMap<>();

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
           //1.4 给各个传感器赋值基准值
                //1.4.1 10个传感器id:1-10
            for (int i = 0; i <10 ; i++) {

                String id = "sensor_" + (i + 1);
                map.put(id,new SensorReading(id,System.currentTimeMillis(),60D+random.nextGaussian()*20));
            }
            while (running){
                //1.4.2 遍历map
//                  另一种
                for (Map.Entry<String, SensorReading> entry : map.entrySet()) {
                    String id = entry.getKey();
                    SensorReading value = entry.getValue();
                    Double temp = value.getTemp();
                    sourceContext.collect(new SensorReading(id,System.currentTimeMillis(),temp+random.nextGaussian()));
                }
//                for (String id : map.keySet()) {
//                    SensorReading sensorReading = map.get(id);
//                    Double temp = sensorReading.getTemp();
//                    SensorReading resultSensor = new SensorReading(id, System.currentTimeMillis(), temp + random.nextGaussian());
//                    sourceContext.collect(resultSensor);
//                }
                Thread.sleep(2000);
            }
        }
        @Override
        public void cancel() {
            running=false;
        }
    }
}
