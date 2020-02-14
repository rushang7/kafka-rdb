package org.egov.kafka.rdb.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    @Autowired
    private MessageService messageService;


    public Long send(String topic, String key, String value) {
        return messageService.send(topic, key, value);
    }

}
