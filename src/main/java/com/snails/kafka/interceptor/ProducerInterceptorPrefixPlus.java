package com.snails.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author snails
 * @Create 2019 - 08 - 19 - 15:52
 * @Email snailsone@gmail.com
 * @desc  自定义拦截器 为每调消息添加另个前缀"prefix2-"
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue="prefix2-"+record.value();
        return new ProducerRecord<String, String>(record.topic(),record.partition(),record.timestamp(),record.key(),modifiedValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
