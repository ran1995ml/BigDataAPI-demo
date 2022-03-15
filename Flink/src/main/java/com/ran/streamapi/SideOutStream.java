package com.ran.streamapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ClassName: SideOutStream
 * Description:processFunction输出到侧输出流
 * 将小于30的数据输出到侧输出流
 * date: 2022/3/5 17:40
 *
 * @author ran
 */
public class SideOutStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
        final OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){};
        SingleOutputStreamOperator<SensorReading> high = source.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getValue() < 40) {
                    context.output(lowTemp, sensorReading);
                } else {
                    collector.collect(sensorReading);
                }
            }
        });
        DataStream<SensorReading> low = high.getSideOutput(lowTemp);
        high.print("high");
//        low.print("low");
        env.execute();
    }
}
