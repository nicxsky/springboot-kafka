package com.example.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {

    /**
     * 指定topic消费
     * topics：监听的topic，可监听多个；
     * topics和topicPartitions不能同时使用；
     * <p>
     * session.timeout.ms的设置过大会导致消费者第一次启动时消费延迟
     *
     * @param record
     */
    @KafkaListener(topics = {"topic1"}, containerFactory = "filterContainerFactory")
    public void onMessage1(ConsumerRecord<?, ?> record) {
        // 消费的哪个topic、partition的消息,打印出消息内容
        //System.out.println("简单消费：" + record.topic() + "-" + record.partition() + "-" + record.value());
        log.info("消费者1==>Topic:[{}], Partition:[{}], offset:[{}], value:[{}]", record.topic(), record.partition(), record.offset(), record.value());
    }

    /**
     * 指定topic、partition、offset消费
     * id：消费者ID；
     * groupId：消费组ID；
     * topicPartitions：可配置更加详细的监听信息，可指定topic、parition、offset监听。
     * onMessage2监听的含义：监听topic1的0号分区，同时监听topic2的0号分区和topic2的1号分区里面offset从8开始的消息。
     *
     * @param record
     */
    @KafkaListener(id = "consumer1", groupId = "felix-group", topicPartitions = {
            @TopicPartition(topic = "topic1", partitions = {"0"}),
            @TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "8"))
    })
    public void onMessage2(ConsumerRecord<?, ?> record) {
        log.info("消费者2==>Topic:[{}], Partition:[{}], offset:[{}], value:[{}]", record.topic(), record.partition(), record.offset(), record.value());
        //System.out.println("topic:" + record.topic() + "|partition:" + record.partition() + "|offset:" + record.offset() + "|value:" + record.value());
    }

    /* 批量消费有问题 TODO
    @KafkaListener(id = "consumer2", groupId = "felix-group2", topics = "topic1")
    public void onMessage3(List<ConsumerRecord<?, ?>> records) {
        System.out.println(">>>批量消费一次，records.size()=" + records.size());
        for (ConsumerRecord<?, ?> record : records) {
            System.out.println(record.value());
        }
    }*/

    /**
     * 模拟消费异常 ConsumerAwareListenerErrorHandler
     *
     * @param record
     * @throws Exception
     */
    /*@KafkaListener(topics = {"topic1"}, errorHandler = "consumerAwareErrorHandler")
    public void onMessage4(ConsumerRecord<?, ?> record) throws Exception {
        throw new Exception("简单消费-模拟异常");
    }*/

    /**
     * TODO 是否可以指定分区？？？
     * 每次启动都会消费一次？？？
     * [2020-11-26T10:44:55.111] [consumer1-0-C-1] INFO - [Consumer clientId=consumer-felix-group-2, groupId=felix-group] Resetting offset for partition topic2-1 to offset 0.
     * [2020-11-26T10:44:55.620] [consumer1-0-C-1] INFO - 消费者2==>Topic:[topic2], Partition:[1], offset:[0], value:[test1-forward message]
     * 消息转发
     * @param record
     * @return
     */
    /*@KafkaListener(topics = {"topic1"})
    @SendTo("topic2")
    public String onMessage7(ConsumerRecord<?, ?> record) {
        return record.value()+"-forward message";
    }*/

}
