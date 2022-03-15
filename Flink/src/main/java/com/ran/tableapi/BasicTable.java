package com.ran.tableapi;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.expressions.In;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * ClassName: BasicTable
 * Description:Table API基本使用
 * date: 2022/3/6 17:57
 *
 * @author ran
 */
public class BasicTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\hello.txt";
        DataStreamSource<String> source = env.readTextFile(path);
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Tuple2<>(s1[0], Integer.parseInt(s1[1]));
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment environment = StreamTableEnvironment.create(env, settings);
        Table table = environment.fromDataStream(dataStream, "name,value");
        Table resultTable = table.select("name,value").filter("name='flink'");

        DataStream<Row> rowDataStream = environment.toAppendStream(resultTable, Row.class);
        rowDataStream.print();
        env.execute();

    }

}
