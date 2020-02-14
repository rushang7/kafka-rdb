package org.egov.kafka.rdb.service;

import lombok.extern.slf4j.Slf4j;
import org.egov.kafka.rdb.model.TopicOffsetLog;
import org.egov.kafka.rdb.model.Message;
import org.egov.kafka.rdb.repository.ConsumerTopicOffsetLogsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class ConsumerTopicOffsetService {

    private Long nullOffsetNumber = 0L;

    @Autowired
    private ConsumerTopicOffsetLogsRepository consumerTopicOffsetLogsRepository;

    public Long recordConsumption(String topic, String consumerGroupId, Message message) {
        TopicOffsetLog topicOffsetLog =
                TopicOffsetLog.builder()
                        .timestamp(System.currentTimeMillis())
                        .topic(topic)
                        .consumerGroupOffset(message.getId())
                        .build();
        topicOffsetLog = consumerTopicOffsetLogsRepository.insertLog(consumerGroupId, topicOffsetLog);
        return topicOffsetLog.getId();
    }

    public Long getLastConsumedOffset(String topic, String consumerGroupId) {
        Optional<TopicOffsetLog> consumerTopicOffsetLog =
                consumerTopicOffsetLogsRepository.getLastLog(consumerGroupId, topic);

        if(consumerTopicOffsetLog.isPresent()) {
            return consumerTopicOffsetLog.get().getConsumerGroupOffset();
        } else {
            return nullOffsetNumber;
        }
    }

}
