package practice;

import bean.WordToOne;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/** 输入数据如下：  使用FlinkSQL实现从Kafka读取数据计算wordcount并将数据写入ES中
 *  hello,atguigu,hello
 *  hello,spark
 *  hello,flink
 */
public class Flink06_SQLTest_my {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.从kafka读取数据
            //所需参数 Properties prop
        Properties prop=new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"mirana1218");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
            //addSource
        DataStreamSource<String> kafkaDStream = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), prop));
        //3.压平带1
        SingleOutputStreamOperator<WordToOne> wordToOne = kafkaDStream.flatMap(new FlatMapFunction<String, WordToOne>() {
            @Override
            public void flatMap(String value, Collector<WordToOne> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(new WordToOne(field, 1L));
                }
            }
        });
        tableEnv.createTemporaryView("word",wordToOne);
        Table sqlresult = tableEnv.sqlQuery("select word,sum(ct) from word group by word");
        tableEnv.connect(new Elasticsearch().version("6")
                        .host("hadoop102", 9200, "http")
                        .index("wordtwst")
                        .documentType("_doc")
                        .bulkFlushMaxActions(1)
                         )
                .inUpsertMode()//TODO 不指定的会报错 Could not find required property 'update-mode'.
                .withFormat(new Json())
                .withSchema(new Schema()
                                .field("word", DataTypes.STRING())
                                .field("ct",DataTypes.BIGINT())
                            )
                .createTemporaryTable("wordcountTable");

        tableEnv.insertInto("wordcountTable",sqlresult);
        env.execute();
    }
}
