package com.snails.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author snails
 * @Create 2019 - 08 - 19 - 15:15
 * @Email snailsone@gmail.com
 * @desc  实现自定义生产者拦截器:(1)实现ProducerInterceptor接口(2)配置拦截器的配置信息
 * 本拦截器的作用,通过onSend()方法为每一条消息添加一个前缀“prefix1-",并通过onAcknowledgement()方法统计计算发送
 * 消息的成功率
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String,String>{
    private volatile long sendSuccess=0;
    private volatile long sendFailure=0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue="prefix1-"+record.value();
        return new ProducerRecord<String, String>(record.topic(),record.partition(),record.timestamp(),record.key(),modifiedValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
           if (e==null){
               sendSuccess++;
           }else {
               sendFailure++;
           }
    }

    @Override
    public void close() {
       double successRatio=(double) sendSuccess/(sendSuccess+sendFailure);
        System.out.println("[INFO]发送成功率="+String.format("%f",successRatio*100)+"%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
