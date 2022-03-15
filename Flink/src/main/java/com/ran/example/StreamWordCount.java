package com.ran.example;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: StreamWordCount
 * Description:流处理单词计数
 * date: 2022/2/13 19:58
 *
 * @author ran
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> source = env.socketTextStream(host, port);
        source.flatMap(new BatchWordCount.MyFlapMapper()).keyBy(0).sum(1).print().setParallelism(1);
        env.execute();
    }
}
