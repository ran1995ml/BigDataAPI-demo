package com.ran.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * ClassName: CallbackProducer
 * Description:带回调函数生产者API
 * 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，分别是
 * RecordMetadata和Exception，如果Exception为 null，说明消息发送成功，
 * 如果Exception不为 null，说明消息发送失败。
 * date: 2022/1/17 22:37
 *
 * @author ran
 */
public class CallbackProducer {
    public static void main(String[] args) {
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
            producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        System.out.println("success->"+recordMetadata.offset());
                    }else{
                        e.printStackTrace();
                    }
                }
            });
        }
        //关闭资源
        producer.close();
    }
}
