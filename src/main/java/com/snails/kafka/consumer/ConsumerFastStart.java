package com.snails.kafka.consumer;

import com.snails.kafka.entity.Company;
import com.snails.kafka.serialization.protostuff.ProtostuffDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

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
        //将自动提交位移 设置为false 默认为true
        //prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return prop;
    }

    public static void main(String[] args) {
        Properties prop = initConfig();
        //创建一个消费者客户端实例
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<String, Company>(prop);
        //KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        //订阅主题使用集合的方式
        //consumer.subscribe(Collections.singletonList(topic));

        //订阅主题使用正则的方式
        //consumer.subscribe(Pattern.compile("topic-.*"));
        //订阅指定的主题的指定分区
        //consumer.assign(Arrays.asList(new TopicPartition("snails",3)));

        //通过PartitionInfo 查询指定主题的元数据信息
        //通过partitionsFor()查询指定主题的元数据信息

     /*   List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo tpinfo : partitionInfos) {
                partitions.add(new TopicPartition(tpinfo.topic(), tpinfo.partition()));
            }
        }
        consumer.assign(partitions);*/

        //消费者消费指定主题分区的消息
        TopicPartition tp = new TopicPartition("snails", 1);
        consumer.assign(Arrays.asList(tp));

        //Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        //循环消费消息 消息消费有两种模式(1)推模式和拉模式
        try {
            while (true) {
                ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(5000));
                //计算消息集中的消息个数
                int count = records.count();
                System.out.println("消息集中消息的size=  "+count);
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

                //使用ConsumerRecords 中的records(String topic) 按照主题维度消费消息
                /*List<String> topics = Arrays.asList(topic);
                for (String topic : topics) {
                    for (ConsumerRecord<String, Company> record : records.records(topic)) {
                        System.out.println(record.topic() + " : " + record.value());
                    }
                }*/

                //消费位移演示
                /*
                TopicPartition tp = new TopicPartition(topic, 1);
                //消费者消费指定主题分区的消息
                consumer.assign(Arrays.asList(tp));**/

                long lastConsumedOffset = -1;//当前消费到的位移
                //判断消息集是否为空
                boolean empty = records.isEmpty();
                if (empty) {
                    System.out.println("消息集合为空........");
                    break;
                }
                List<ConsumerRecord<String, Company>> partitionRecords = records.records(tp);
                lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                for (ConsumerRecord<String, Company> record:partitionRecords) {
                    //todo some logical process
                    System.out.println("topic-"+record.topic()+"partition-"+record.partition()+"offset-"+record.offset()+"msg= "+record.value());
                }
                consumer.commitAsync();//同步提交消费位移
                System.out.println("consumed offset is" + lastConsumedOffset);
                OffsetAndMetadata committed = consumer.committed(tp);
                //下一次拉取消息的位置position
                long committerOffset = committed.offset();
                System.out.println("commited offset is " + committerOffset);
                long consumerPosition = consumer.position(tp);
                System.out.println("the iffset of the next record is " + consumerPosition);


                //同步提交方式一:先对拉取到的每一条消息做相应的处理 然后对整个消息集做同步提交 示例如下
                /*for (ConsumerRecord<String, Company> record : records) {
                    //todo some logical processing
                }
                consumer.commitSync();*/

                //同步提交方式二:将拉取到的消息放到一个缓存buffer，当buffer积累到足够多的时候，再做相应的批量处理.之后再做批量提交位移
                /*final int minBatchSize=200;
                List<ConsumerRecord < String, Company>> buffer=new ArrayList<> ();
                for (ConsumerRecord<String, Company> record : records) {
                    buffer.add(record);
                }
                if (buffer.size()>=minBatchSize){
                    //todo some logical processing with buffer
                    consumer.commitSync();
                    buffer.clear();
                }*/

                /**
                 * 以上两种同步提交的方式  都存在重复消费的问题，如果在业务逻辑处理后，并且在同步位移提交前，程序
                 * 崩溃，那么待恢复之后，只能从上一次位移提交的地方拉取消息
                 */


                /**
                 * 无参的commitSync()方法是只能提交当前批次的position值，kafka提供了一个有参的commitSync(final Map<TopicPartition,OffsetAndMetadata> offsets)方法
                 * 用来提交指定分区的位移，这样可以做到需要提交一个中间值的位移，符合这样的场景：业务每消费一条消息就提交一次位移
                 * 实际情况很少会这么用 ,commitSync本身是同步 比较消耗性能
                 */
               /* for (ConsumerRecord<String, Company> record : records) {
                    //todo some logical process
                    long offset = record.offset();
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
                }*/


                /**
                 * 实际应用中更多的是按照分区的粒度划分提交位移的界限
                 * 这里会用到ConsumerRecords类的partitions()方法和records(TopicPartition)方法
                 */

                /*for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, Company>> partitionRecords2 = records.records(partition);
                    for (ConsumerRecord<String, Company> record : partitionRecords2) {
                        //todo some logical processing
                    }
                    long lastConsumeOffset = partitionRecords2.get(partitionRecords2.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumeOffset + 1)));
                }*/


                /**
                 * 异步提交位移 commitAsync 在执行时 消费者线程不会被阻塞，可能在提交位移消费的结果未返回就开始了新一次的拉取操作
                 * 异步提交可以提高可使消费者的性能得到提高，对应的有三个重载方法
                 *    (1)commitAsync()
                 *    (2)commitAsync(OffsetCommitCallback callback)
                 *    (3)commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,OffsetCommitCallback callback)
                 */



                /*for (ConsumerRecord<String, Company> record : records) {
                    //todo some logical process
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null) {
                            System.out.println(offsets);
                        } else {
                            logger.error("fail to commit offsets {}", offsets, exception);
                        }
                    }
                });*/


            }
        } catch (Exception e) {
            logger.error("occur exception ", e);
        } finally {
            //关闭消费者实例
            consumer.close();
        }

    }
}
