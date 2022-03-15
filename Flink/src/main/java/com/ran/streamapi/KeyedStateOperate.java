package com.ran.streamapi;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: KeyedStateOperate
 * Description:使用KeyedState，若两个值的差超过5报警
 * date: 2022/3/6 15:47
 *
 * @author ran
 */
public class KeyedStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
        source.keyBy((KeySelector<SensorReading, String>) SensorReading::getName).flatMap(new IncreasingWarning(0)).print();
        env.execute();
    }
    public static class IncreasingWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Integer,Integer>>{

        private Integer threshold;
        private ValueState<Integer> lastValue;

        IncreasingWarning(Integer threshold){
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastValue = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-value",Integer.class,Integer.MIN_VALUE));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
            int value = lastValue.value()==null?Integer.MIN_VALUE: lastValue.value();
            lastValue.update(sensorReading.getValue());
            if(value!=Integer.MIN_VALUE){
                int diff = Math.abs(value-sensorReading.getValue());
                if(diff==threshold){
                    collector.collect(new Tuple3<>(sensorReading.getName(),sensorReading.getValue(),value));
                }
            }
        }


    }
}
