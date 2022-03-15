package com.ran.tableapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * ClassName: WindowTable
 * Description:SQL开窗操作
 * date: 2022/3/12 15:56
 *
 * @author ran
 */
public class WindowTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(path);
        SingleOutputStreamOperator<Word> dataStream = inputStream.map(new MapFunction<String, Word>() {
            @Override
            public Word map(String s) throws Exception {
                String[] words = s.split(" ");
                return new Word(words[0], Integer.parseInt(words[1]));
            }
        });
        Table table = tableEnv.fromDataStream(dataStream, "name,value,pt.proctime");
        table.window(Tumble.over("2.seconds").on("pt").as("tw"))
                .groupBy("name,tw").select("name,name.count,value.avg,tw.end");
        tableEnv.toAppendStream(table,Row.class).print("result");
        env.execute();

    }
}
