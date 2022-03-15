package com.ran.streamapi;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * ClassName: Watermark
 * Description:
 * date: 2022/2/28 22:27
 *
 * @author ran
 */
public class WatermarkStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySource.MySourceFunction());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimeStamp() * 1000L;
            }
        });
    }

    //周期性生成watermark
    public static class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading>{

        private long bound = 60 * 1000L;      //延迟一分钟
        private long maxTs = Long.MIN_VALUE;  //当前最大时间戳

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTs-bound);
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            maxTs = Math.max(maxTs,sensorReading.getTimeStamp());
            return sensorReading.getTimeStamp();
        }
    }

    //间断生成watermark
    public static class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading>{

        private long bound = 60 * 1000L;      //延迟一分钟

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading sensorReading, long l) {
            if(sensorReading.getName().equals("sensor_1"))
                return new Watermark(l-bound);
            return null;
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            return sensorReading.getTimeStamp();
        }
    }
}
