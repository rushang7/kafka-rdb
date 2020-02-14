package org.egov.kafka.rdb.service;

import lombok.extern.slf4j.Slf4j;
import org.egov.kafka.rdb.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class ConsumerService {

    @Autowired
    private MessageService messageService;
    @Autowired
    private ConsumerTopicOffsetService consumerTopicOffsetService;

    public Optional<Message> getNextMessageFor(String topic, String consumerGroupId) {
        Long offset = consumerTopicOffsetService.getLastConsumedOffset(topic, consumerGroupId);
        Optional<Message> message = messageService.readNextMessageAfterOffset(topic, offset);
        if(message.isPresent()) {
            consumerTopicOffsetService.recordConsumption(topic, consumerGroupId, message.get());
            log.info(message.get().getId() + " message of topic : " + topic + " consumed by " + consumerGroupId);
        }
        return message;
    }

}
