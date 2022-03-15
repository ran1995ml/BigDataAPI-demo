package com.ran.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: AsyncConsumer
 * Description:异步手动提交offset
 * 先提交offset后消费，可能漏消费；先消费后提交offset，可能重复消费
 * date: 2022/1/17 23:08
 *
 * @author ran
 */
public class AsyncConsumer {
    public static void main(String[] args) {
        //创建Kafka生产者配置信息
        Properties properties = new Properties();
        //kafka 集群，broker-list
        properties.put("bootstrap.servers", "node1:9092");
        properties.put("group.id","test");
        properties.put("enable.auto.commit","false");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test1"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(),record.key(),record.value());
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(e!=null){
                        System.err.println("Commit failed for "+map);
                    }
                }
            });
        }
    }
}
