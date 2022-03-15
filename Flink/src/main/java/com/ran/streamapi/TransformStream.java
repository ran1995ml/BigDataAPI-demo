package com.ran.streamapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: TransformStream
 * Description:转换算子
 * date: 2022/2/26 18:46
 *
 * @author ran
 */
public class TransformStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> source = env.socketTextStream(host, port);
        source.flatMap(new SplitWordFunction()).map(new WordValueMapFunction()).filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
                return !tuple.f0.equals("spark");
            }
        }).keyBy(new WordGroupByFunction()).reduce(new WordReduceFunction()).print();
        env.execute();
    }

    public static class SplitWordFunction implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for(String word:words){
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }

    public static class WordValueMapFunction implements MapFunction<Tuple2<String, Integer>,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> word) throws Exception {
            return new Tuple2<>(word.f0, word.f1 * 10);
        }
    }

    public static class WordGroupByFunction implements KeySelector<Tuple2<String, Integer>, String>{

        @Override
        public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
            return tuple2.f0;
        }
    }

    public static class WordReduceFunction implements ReduceFunction<Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
            return new Tuple2<String,Integer>(t1.f0,t1.f1+t2.f1);
        }
    }

}
