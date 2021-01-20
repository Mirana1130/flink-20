package day03;

//import bean.SensorReading;
//import org.apache.flink.api.common.functions.RuntimeContext;
//
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//public class Flink01_Sink_ES {
//    public static void main(String[] args) {
//        //1.获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //env.setParallelism(1);
//        //2.从文件读取数据创建流
//        DataStreamSource<String> socketDStream = env.socketTextStream("hadoop102", 9999);
//
//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop102", 9200));
//      //new ElasticsearchSink.Builder<String>(httpHosts, new MyEsSinkFunction()).build();
////        socketDStream.addSink( );
//
//        // socketDStream.addSink(new Elast)
//    }
//    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
//        @Override
//        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
//
//            HashMap<String, String> dataSource = new HashMap<>();
//            dataSource.put("id", element.getId());
//            dataSource.put("ts", element.getTs().toString());
//            dataSource.put("temp", element.getTemp().toString());
//
//            IndexRequest indexRequest = Requests.indexRequest()
//                    .index("sensor")
//                    .type("readingData")
//                    .source(dataSource);
//
//            indexer.add(indexRequest);
//        }
//    }
//}
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Flink01_Sink_ES {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.写入ES
        //3.1 准备集群连接参数
        //3.2 创建Es Sink Builder
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        ElasticsearchSink.Builder<String> stringBuilder = new ElasticsearchSink.Builder<>(httpHosts, new MyEsSinkFunction());
        //TODO 3.3 设置刷写条数
        stringBuilder.setBulkFlushInterval(1);
        ElasticsearchSink<String> esSearchSink = stringBuilder.build();

        //3.4 创建EsSink
        socketTextStream.addSink(esSearchSink);
        //4.执行
        env.execute();
    }
public static class MyEsSinkFunction implements ElasticsearchSinkFunction<String>{
    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        Map<String, String> map = new HashMap<>();
        String[] fields = element.split(",");
        map.put("id",fields[0]);
        map.put("ts",fields[1]);
        map.put("temp",fields[2]);
        IndexRequest source = Requests.indexRequest().index("sensor_my").type("_doc").source(map);
        indexer.add(source);
    }
}
}