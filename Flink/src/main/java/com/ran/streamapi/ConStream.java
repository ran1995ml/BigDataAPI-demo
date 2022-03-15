package com.ran.streamapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * ClassName: ConStream
 * Description:合并流connect和union
 * date: 2022/2/26 22:08
 *
 * @author ran
 */
public class ConStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
        SplitStream<SensorReading> split = source.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getValue() > 20 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        ConnectedStreams<SensorReading, SensorReading> connect = high.connect(low);
        SingleOutputStreamOperator<Object> coStream = connect.map(new CoMapFunction<SensorReading, SensorReading, Object>() {
            @Override
            public Object map1(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getName(), sensorReading.getValue(), "warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getName(), sensorReading.getValue());
            }
        });
        DataStream<SensorReading> unionStream = high.union(low);
        unionStream.print();
        env.execute();
    }
}
