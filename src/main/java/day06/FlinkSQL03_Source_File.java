package day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;


/**
 * TODO 从文件系统读取文件 打印到控制台
 */
public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO  2.File System Connector (Source:Batch , Streaming Append Mode ; Sink：Batch, Streaming Append Mode; Format: OldCsv-only
        tableEnv.connect(new FileSystem().path("sensor"))//读取sensor目录文件
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("temp",DataTypes.DOUBLE())
                            )
                .createTemporaryTable("sensor");
        Table sqlresult = tableEnv.sqlQuery("select id,temp from sensor where id = 'sensor_1'");
        Table sensortable = tableEnv.from("sensor");
        Table tableresult = sensortable.select("id,temp").where(" id = 'sensor_7'");

        //打印  撤回流
        //tableEnv.toRetractStream(sqlresult, Row.class).print("sql");
        //tableEnv.toRetractStream(tableresult, Row.class).print("table");

//打印结果 //table> (true,sensor_7,6.7)
          //sql> (true,sensor_1,35.8)
          //sql> (true,sensor_1,38.3)


        //打印  追加流
        tableEnv.toAppendStream(sqlresult, Row.class).print("sql");
        tableEnv.toAppendStream(tableresult, Row.class).print("table");
//打印结果 //sql> sensor_1,35.8
          //table> sensor_7,6.7
          //sql> sensor_1,38.3

        env.execute();
    }
}
//文件
//sensor_1,1547718199,35.8
//sensor_1,1547718199,38.3
//sensor_6,1547718201,15.4
//sensor_7,1547718202,6.7
//sensor_10,1547718205,38.1