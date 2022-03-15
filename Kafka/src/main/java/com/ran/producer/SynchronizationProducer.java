package com.ran.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * ClassName: SynchronizationProducer
 * Description:同步发送生产者API
 * 一条消息发送之后，会阻塞当前线程，直至返回ack
 * send方法返回的是一个 Future对象，调用Future对象的get方法即可实现同步
 * date: 2022/1/17 22:42
 *
 * @author ran
 */
public class SynchronizationProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //创建Kafka生产者配置信息
        Properties properties = new Properties();
        //kafka 集群，broker-list
        properties.put("bootstrap.servers", "node1:9092");
        //ACK应答级别
        properties.put("acks", "all");
        //重试次数
        properties.put("retries", 1);
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //发送数据
        for(int i=0;i<15;i++){
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i))).get();
        }
        //关闭资源
        producer.close();
    }
}
