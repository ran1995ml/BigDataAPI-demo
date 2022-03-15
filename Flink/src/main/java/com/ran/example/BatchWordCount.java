package com.ran.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: BatchWordCount
 * Description:批处理单词计数
 * date: 2022/2/13 17:58
 *
 * @author ran
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\hello.txt";
        DataSource<String> source = env.readTextFile(path);
        source.flatMap(new MyFlapMapper()).groupBy(0).sum(1).print();
    }

    public static class MyFlapMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for(String word:words){
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
