package com.snails.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author snails
 * @Create 2019 - 08 - 19 - 11:38
 * @Email snailsone@gmail.com
 * @desc  自定义分区器
 * 应用场景:根据自身的业务需求来灵活的分配分区的计算方式，如一般的大型电商都有多个仓库
 *        可以将仓库的名称或ID作为key来灵活记录商品的信息
 * 实现自定义的Partitioner后需要通过配置参数partitioner.class来显示指定这个分区器
 * prop.put(ProducerConfig.PRATITIONER_CLASS_CONFIG,CustomPartitioner.class.getName());
 */
public class CustomPartitioner implements Partitioner {
    private final AtomicInteger counter=new AtomicInteger(0);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /**
         * 根据主题获取此主题的分区列表
         */
        List<PartitionInfo> partitionInfos=cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        if (keyBytes==null){
            return counter.getAndIncrement()%numPartitions;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
