package day07;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class FlinkSQL21_Function_UDAF {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.注册函数
        tableEnv.registerFunction("tempAvg", new TempAvg());

        //5.TableAPI 使用UDAF
        Table tableresult = table.groupBy("id").select("id,temp.tempAvg");

        //6.SQL 方式使用UDAF
        Table sqlresult = tableEnv.sqlQuery("select id,tempAvg(temp) from " + table + " group by id");

        //7.转换为流进行打印数据
        tableEnv.toRetractStream(tableresult, Row.class).print("Table");
        tableEnv.toRetractStream(sqlresult, Row.class).print("SQL");

        //8.执行
        env.execute();
    }
    public static class TempAvg extends AggregateFunction<Double,Tuple2<Double,Long>>{
        //返回值是结果数据的方法 用来最终计算结果
        @Override
        public Double getValue(Tuple2<Double, Long> doubleLongTuple2) {
            return doubleLongTuple2.f0/doubleLongTuple2.f1;
        }
        //返回值是缓存数据的方法 用来初始化缓存数据
        @Override
        public Tuple2<Double, Long> createAccumulator() {
            return new Tuple2<>(0.0,0L);
        }
        //TODO accumulate方法每进来一条数据,触发计算
        public void accumulate(Tuple2<Double,Long> acc,Double value){
            acc.f0+=value;
            acc.f1+=1;
        }
    }
}