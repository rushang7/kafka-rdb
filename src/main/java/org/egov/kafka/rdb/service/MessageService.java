package org.egov.kafka.rdb.service;

import org.egov.kafka.rdb.model.Message;
import org.egov.kafka.rdb.repository.MessageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class MessageService {

    @Autowired
    private MessageRepository messageRepository;

    public Long send(String topic, String key, String value) {
        Message message = Message.builder().timestamp(System.currentTimeMillis()).key(key).value(value).build();

        message = messageRepository.insertMessage(topic, message);

        return message.getId();
    }

    public Optional<Message> readNextMessageAfterOffset(String topic, Long offset) {
        return messageRepository.readMessageAfterOffset(topic, offset);
    }

}
