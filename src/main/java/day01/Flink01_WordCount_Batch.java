package day01;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//TODO 批处理
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建环境 用的 ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.读取数据 fromCollection()从集合 fromElements()可直接传元素 readFile(要传FileInputFormat)读文件 readCsvFile()读Csv文件
        //  readTextFile() 读取文本数据 一行一行读 hello mirana flink
        DataSource<String> input = env.readTextFile("input");
        //TODO 3.拍平数据 flatMap ( FlatMapFuntion<String,Obejet> flatMapper) String是输入类型，Obejet是返回值类型
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = input.flatMap(new MyFlatMapFunc());
        //TODO 4.分组 groupBy() 元组可用位置 javabean可用属性名来分组
        UnsortedGrouping<Tuple2<String, Integer>> groupby = wordToOne.groupBy(0);
        //TODO 5.计算wordcount
        AggregateOperator<Tuple2<String, Integer>> result = groupby.sum(1);
        //TODO 6.打印
        result.print();
        //7.不需要关闭
    }
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override //因为一进多出，没法给定返回值，遍历写出
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按空格切分
            String[] words = s.split(" ");
            //遍历写出
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
