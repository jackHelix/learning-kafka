package com.snails.kafka.consumer;

import com.snails.kafka.entity.Company;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author snails
 * @Create 2019 - 07 - 31 - 10:39
 * @Email snailsone@gmail.com
 * @desc Kafka 消费者实例
 */
public class ConsumerFastStart {
    public static final String brokerList = "120.79.31.243:9092";
    public static final String topic = "snails";
    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.put("value.deserializer", "com.snails.kafka.serialization.CompanyDeserializer");
        properties.put("bootstrap.servers", brokerList);
        //设置消费组名称
        properties.put("group.id", groupId);
        //创建一个消费者客户端实例
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(properties);
        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        //Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        //循环消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Company> record:records){
                System.out.println(record.value());
            }
        }

    }
}
