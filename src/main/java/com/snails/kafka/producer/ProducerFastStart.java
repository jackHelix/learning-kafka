package com.snails.kafka.producer;

import com.snails.kafka.entity.Company;
import com.snails.kafka.serialization.protostuff.ProtostuffSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author snails
 * @Create 2019 - 07 - 31 - 10:25
 * @Email snailsone@gmail.com
 * @desc Kafka生产者 实例
 */
public class ProducerFastStart {
    public static final String brokerList = "120.79.31.243:9092";
    public static final String topic = "snails";
    public static final String clientId = "producer.client.id.demo";

    public static Properties initConfig() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
        //设置生产者客户端名称
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        //配置可重试异常的重试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 10);
        //配置自定义的分区器
        //prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class.getName());
        //配置自定义生产者拦截器
        //prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ProducerInterceptorPrefix.class.getName()+","+ ProducerInterceptorPrefixPlus.class.getName());
        /**指定分区中有多少个副本收到这天消息，之后生产者才认为这条消息被成功写入（成功写入会有来自服务器的成功响应）
         * acks=1:只要有leader的副本成功写入消息 就收到来自服务器的成功响应 是消息可靠性和吞吐量的折中方案
         * acks=0:生产者发送消息之后不需要等到服务器响应，可以达到大吞吐量
         * acks=-1 or acks=all 需要等到所有ISR中的副本都成功写入并收到来自服务器的成功响应
         */
        prop.put(ProducerConfig.ACKS_CONFIG, "0");
        return prop;
    }

    public static void main(String[] args) {
       /* Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);*/

        Properties properties = initConfig();
        //配置生产者客户端参数并创建KafkaProducer实例
        //KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //构建所需要发送的消息
        //final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "kafka hello world");
        Company data = Company.builder().name("中国移动").address("广州天河区大观路").extend("拓展字段").build();
        KafkaProducer<String, Company> producer = new KafkaProducer<String, Company>(properties);
        ProducerRecord<String, Company> record = new ProducerRecord<String, Company>(topic, data);
        //发送消息
        try {
            //发送的消息模式1 发后即忘
            //producer.send(record);

            //发送的消息模式2 同步发送
            //producer.send(record).get();

            /*Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata.topic()+"-"+recordMetadata.partition()+"-"+recordMetadata.offset());
            */

            //发送消息模式3 异步发送
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + "-" + recordMetadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        //关闭生产者实例
        producer.close();
    }
}
