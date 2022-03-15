package com.ran.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * ClassName: SyncConsumer
 * Description:手动提交offset，分为同步提交和异步提交
 * 都会将本次poll的一批数据最高的偏移量提交
 * 同步提交会阻塞线程，一直到提交成功，失败会自动重试
 * 异步没有重试机制，可能会提交失败
 * date: 2022/1/17 22:59
 *
 * @author ran
 */
public class SyncConsumer {
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
            consumer.commitSync();
        }
    }
}
