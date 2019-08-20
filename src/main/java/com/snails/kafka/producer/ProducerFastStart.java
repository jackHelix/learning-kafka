package com.snails.kafka.producer;

import com.snails.kafka.entity.Company;
import com.snails.kafka.interceptor.ProducerInterceptorPrefix;
import com.snails.kafka.interceptor.ProducerInterceptorPrefixPlus;
import com.snails.kafka.partitioner.CustomPartitioner;
import com.snails.kafka.serialization.CompanySerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.util.Properties;
import java.util.concurrent.Future;

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
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        //配置可重试异常的重试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 10);
        //配置自定义的分区器
        //prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class.getName());
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ProducerInterceptorPrefix.class.getName()+","+ ProducerInterceptorPrefixPlus.class.getName());
        return prop;
    }

    public static void main(String[] args) {
       /* Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);*/

        Properties properties = initConfig();
        //配置生产者客户端参数并创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //构建所需要发送的消息
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "success ....");

        /*Company data = Company.builder().name("中国移动").address("广州天河区大观路").build();
        KafkaProducer<String, Company> producer = new KafkaProducer<String, Company>(properties);
        ProducerRecord<String, Company> record=new ProducerRecord<String, Company>(topic,data);*/
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
            e.printStackTrace();
        }
        //关闭生产者实例
        producer.close();
    }
}
