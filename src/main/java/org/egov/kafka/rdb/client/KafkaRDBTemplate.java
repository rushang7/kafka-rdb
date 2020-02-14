package org.egov.kafka.rdb.client;

import org.egov.kafka.rdb.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaRDBTemplate {

    @Autowired
    private ProducerService producerService;

    public void send(String topic, String key, String value) {
        producerService.send(topic, key, value);
    }

}
