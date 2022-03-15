package com.ran.streamapi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * ClassName: MySource
 * Description:自定义source
 * date: 2022/2/26 11:28
 *
 * @author ran
 */
public class MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new MySourceFunction());
        source.print();
        env.execute();
    }
    public static class MySourceFunction implements SourceFunction<SensorReading>{

        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            Random random = new Random();
            HashMap<String, Integer> map = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                map.put("sensor"+(i+1),random.nextInt(50));
            }
            while (running){
                for(String sensorId:map.keySet()){
                    int value = map.get(sensorId);
                    sourceContext.collect(new SensorReading(sensorId,value,System.currentTimeMillis()));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
