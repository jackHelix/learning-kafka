package com.snails.kafka.consumer;

import com.snails.kafka.entity.Company;
import com.snails.kafka.serialization.protostuff.ProtostuffDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @Author snails
 * @Create 2019 - 07 - 31 - 10:39
 * @Email snailsone@gmail.com
 * @desc Kafka 消费者实例
 */
public class ConsumerFastStart {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerFastStart.class);
    public static final String brokerList = "120.79.31.243:9092";
    public static final String topic = "snails";
    public static final String groupId = "group.demo";

    //配置消费者客户端参数
    public static Properties initConfig() {
    /*    Properties prop = new Properties();
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //prop.put("value.deserializer", "com.snails.kafka.serialization.CompanyDeserializer");
        prop.put("bootstrap.servers", brokerList);
        //设置消费组名称
        prop.put("group.id", groupId);
        //设置KafkaConsumenr对应的客户端id
        prop.put("client.id","cousumer.client.id.demo");*/

        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //prop.put("value.deserializer", "com.snails.kafka.serialization.CompanyDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtostuffDeserializer.class.getName());
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //设置消费组名称
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置KafkaConsumenr对应的客户端id
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "cousumer.client.id.demo");
        return prop;
    }

    public static void main(String[] args) {
        Properties prop = initConfig();
        //创建一个消费者客户端实例
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(prop);
        //KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        //订阅主题使用集合的方式
        consumer.subscribe(Collections.singletonList(topic));
        //订阅主题使用正则的方式
        //consumer.subscribe(Pattern.compile("topic-.*"));
        //订阅指定的主题的指定分区
        //consumer.assign(Arrays.asList(new TopicPartition("snails",0)));

        //通过PartitionInfo 查询指定主题的元数据信息
        //通过partitionsFor()查询指定主题的元数据信息

        /*List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo tpinfo : partitionInfos) {
                partitions.add(new TopicPartition(tpinfo.topic(), tpinfo.partition()));
            }
        }
        consumer.assign(partitions);*/

        //Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        //循环消费消息 消息消费有两种模式(1)推模式和拉模式
        try {
            while (true) {
                ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
                //计算消息集中的消息个数
                int count = records.count();
                //判断消息集是否为空
                boolean empty = records.isEmpty();
                //ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                /*for (ConsumerRecord<String, Company> record : records) {
                    //for (ConsumerRecord<String, String> record : records) {
                    //打印消息所属   主题|分区|偏移量|
                    System.out.println("topic= " + record.topic() + ", parttition= " + record.partition() + ", offset =" + record.offset());
                    //打印消费消息
                    System.out.println(record.value());
                }*/

                //获取ConsumerRecords消息集中指定分区的消息 调用方法records(TopicPartition partition) 获取消息集所有分区:records.partitions()
                /*for (TopicPartition tp : records.partitions()) {
                    for (ConsumerRecord<String, Company> record : records.records(tp)) {
                        //for (ConsumerRecord<String, String> record : records) {
                        //打印消息所属   主题|分区|偏移量|
                        System.out.println("topic= " + record.topic() + ", parttition= " + record.partition() + ", offset =" + record.offset());
                        //打印消费消息
                        System.out.println(record.value());
                    }
                }*/

                //使用ConsumerRecords 中的record(String topic) 按照主题维度消费消息
                List<String> topics = Arrays.asList(topic);
                for (String topic : topics) {
                    for (ConsumerRecord<String, Company> record : records.records(topic)) {
                        System.out.println(record.topic() + " : " + record.value());
                    }
                }

            }
        } catch (
                Exception e)

        {
            logger.error("occur exception ", e);
        } finally

        {
            //关闭消费者实例
            consumer.close();
        }

    }
}
