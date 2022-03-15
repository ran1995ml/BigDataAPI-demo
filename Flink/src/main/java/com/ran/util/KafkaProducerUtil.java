package com.ran.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * ClassName: KafkaProducerUtil
 * Description:数据写入Kafka
 * date: 2022/3/13 23:04
 *
 * @author ran
 */
public class KafkaProducerUtil {
    public static void writeToKafka(String topic) throws IOException {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\UserBehavior.csv";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while((line=bufferedReader.readLine())!=null){
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            producer.send(record);
        }
        producer.close();
    }
}
