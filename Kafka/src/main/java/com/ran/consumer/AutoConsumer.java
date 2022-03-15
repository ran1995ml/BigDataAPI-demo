package com.ran.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * ClassName: AutoConsumer
 * Description:自动提交offset
 * 基于时间提交，难以把握提交时机
 * date: 2022/1/17 22:47
 *
 * @author ran
 */
public class AutoConsumer {
    public static void main(String[] args) {
        //创建Kafka生产者配置信息
        Properties properties = new Properties();
        //kafka 集群，broker-list
        properties.put("bootstrap.servers", "node1:9092");
        properties.put("group.id","test");
        properties.put("enable.auto.commit","true");
        properties.put("auto.commit.interval.ms","1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test1"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(),record.key(),record.value());
            }
        }
    }
}
