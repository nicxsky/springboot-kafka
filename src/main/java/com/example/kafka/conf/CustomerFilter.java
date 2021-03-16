package com.example.kafka.conf;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

/**
 * 自定义过滤器，拦截在customer之前，true抛弃，false正常抵达消费监听器
 */
@Component
@Slf4j
public class CustomerFilter {
    @Autowired
    ConsumerFactory consumerFactory;

    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        // 被过滤的消息将被丢弃
        factory.setAckDiscarded(true);
        // 消息过滤策略
        factory.setRecordFilterStrategy(consumerRecord -> {
            if (consumerRecord.value().equals("test")) {
                log.info("被过滤的消息: [{}]", consumerRecord.value());
                return true;
            }
            return false;
        });
        return factory;
    }


}
