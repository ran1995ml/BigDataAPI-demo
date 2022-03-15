package com.ran.tableapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * ClassName: FileTable
 * Description:sql语句直接查询，POJO直接映射成表的schema
 * date: 2022/3/6 21:26
 *
 * @author ran
 */
public class FileTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\hello.txt";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStreamSource<String> source = env.readTextFile(path);
        SingleOutputStreamOperator<Word> dataStream = source.map(new MapFunction<String, Word>() {
            @Override
            public Word map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Word(s1[0], Integer.parseInt(s1[1]));
            }
        });
        Table table = tableEnv.fromDataStream(dataStream, "name,value");
        tableEnv.createTemporaryView("table1",table);
        Table result = tableEnv.sqlQuery("select distinct name from table1 where name<>'flink'");
        tableEnv.toRetractStream(result,TypeInformation.of(String.class)).print();
        env.execute();
    }

}
