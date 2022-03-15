package com.ran.streamapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * ClassName: WinStream
 * Description:滚动窗口
 * date: 2022/2/27 21:55
 *
 * @author ran
 */
public class WinStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
        source.map(new MapFunction<SensorReading, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getName(),sensorReading.getValue());
            }
        }).keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0).timeWindow(Time.seconds(10)).minBy(1).print();
        env.execute();
    }
}
