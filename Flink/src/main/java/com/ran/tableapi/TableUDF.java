package com.ran.tableapi;

import com.ran.tableapi.udf.AvgValue;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: TableUDF
 * Description:
 * date: 2022/3/12 16:48
 *
 * @author ran
 */
public class TableUDF {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        Table table = tableEnv.fromDataStream(dataStream, "name,value");
        tableEnv.createTemporaryView("source",table);
//        MyHashCode hashCode = new MyHashCode(23);
//        tableEnv.registerFunction("myHashCode",hashCode);
//        Table result = table.select("name,value,myHashCode(name)");
//        tableEnv.toAppendStream(result, Row.class).print("result");

//        Split split = new Split("_");
//        tableEnv.registerFunction("split",split);
//        Table result1 = table.joinLateral("split(name) as (word,length)")
//                .select("name,word,length");
//        tableEnv.toAppendStream(result1, Row.class).print("result1");
//        Table sqlTable1 = tableEnv.sqlQuery("select name,word,length "
//                + "from source,LATERAL TABLE(split(name)) as splitId(word,length)");
//        tableEnv.toAppendStream(sqlTable1, Row.class).print("sqlTable1");


        AvgValue avgValue = new AvgValue();
        tableEnv.registerFunction("avgValue",avgValue);
        Table avgTable = table.groupBy("name").aggregate("avgValue(value) as avg").select("name,avg");
        tableEnv.toRetractStream(avgTable,Row.class).print("avgTable");

        env.execute();
    }



}