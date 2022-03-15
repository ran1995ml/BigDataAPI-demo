package com.ran.streamapi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * ClassName: SourceCollection
 * Description:从集合读取数据
 * date: 2022/2/26 10:35
 *
 * @author ran
 */
public class SourceCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.fromCollection(Arrays.asList(new SensorReading("sensor1", 35),
                new SensorReading("sensor2", 40)));
        source.print();
        env.execute();
    }

}
