package com.ran.streamapi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * ClassName: KeyedStreamFunction
 * Description:KeyedProcessFunction使用
 * 监控value,如果在3s内连续上升则报警
 * date: 2022/2/28 23:11
 *
 * @author ran
 */
public class KeyedStreamFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        source.keyBy((KeySelector<SensorReading, String>) SensorReading::getName).process(new TempIncreaseWarning(3)).print();
        env.execute();
    }

    private static class TempIncreaseWarning extends KeyedProcessFunction<String,SensorReading,String> {

        private Integer interval;
        private ValueState<Integer> lastTempState;
        private ValueState<Long> timerTsState;

        public TempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-temp",Integer.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
            //取出状态
            Integer lastTemp = lastTempState.value() == null? 0: lastTempState.value();
            Long timerTs = timerTsState.value();

            //更新温度状态
            lastTempState.update(sensorReading.getValue());
            if(sensorReading.getValue()>lastTemp&&timerTs==null){
                long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }else if(sensorReading.getValue()<lastTemp&&timerTs!=null){
                context.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+":"+interval);
            timerTsState.clear();
        }
    }
}
