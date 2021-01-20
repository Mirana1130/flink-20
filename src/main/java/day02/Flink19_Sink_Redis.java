package day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
TODO redis sink test
 */
public class Flink19_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//方便看结果
        //2.从hadoop102 9999端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //TODO 3.将数据写入Redis -> addSink(ctrl+P)->sinkFunction-> new RedisSink(ctrl+P)-> (FlinkJedisConfigBase,RedisMapper)
        //  ->(FlinkJedisPoolConfig.Build().build, public static class my RedisMapper)
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();

        socketTextStream.addSink(new RedisSink(flinkJedisPoolConfig,new MyRedisMapper()));
        //4.执行任务
        env.execute();
    }
    public static class MyRedisMapper implements RedisMapper<String>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            //TODO new RedisCommandDescription(ctrl+P)-> (RedisCommand,String(此参数可选)
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor_my");
        }
        @Override
        public String getKeyFromData(String data) {
            String[] fields = data.split(",");
            return fields[0];
        }

        @Override
        public String getValueFromData(String data) {
            String[] fields = data.split(",");
            return fields[2];
        }
    }
}