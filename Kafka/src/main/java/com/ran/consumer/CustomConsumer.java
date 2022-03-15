package com.ran.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * ClassName: CustomConsumer
 * Description:自定义存储offset
 * date: 2022/1/17 23:15
 *
 * @author ran
 */
public class CustomConsumer {
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();
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
        consumer.subscribe(Arrays.asList("test1"), new ConsumerRebalanceListener() {
            //reblance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                commitOffset(currentOffset);
            }
            //reblance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                currentOffset.clear();
                for(TopicPartition partition:collection){
                    consumer.seek(partition,getOffset(partition));
                }
            }
        });
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(),record.key(),record.value());
            }
            consumer.commitSync();
        }
    }
    //获取某分区最新offset
    private static long getOffset(TopicPartition partition){
        return 0;
    }
    //提交该消费者所在分区offset
    private static void commitOffset(Map<TopicPartition,Long> currentOffset){

    }
}
