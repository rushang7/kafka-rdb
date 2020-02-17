package org.egov.kafka.rdb;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.kafka.rdb.client.KafkaConsumer;
import org.egov.kafka.rdb.client.KafkaRDBTemplate;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.listener.MessageListener;

import javax.annotation.PostConstruct;
import java.util.Optional;

@Slf4j
@SpringBootApplication
public class Main {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Main.class, args);
    }

    @Autowired
    private KafkaRDBTemplate kafkaRDBTemplate;
    @Autowired
    private KafkaConsumer kafkaConsumer;

    String topic = "asd.asd-zxc";
    String topic2 = "update-pgr";
    String persisterConsumerGroupId = "egov-persister";
    String indexerConsumerGroupId = "egov-indexer";

    @PostConstruct
    public void init() throws SchedulerException, InterruptedException {

        kafkaRDBTemplate.send(topic, null, "zxc");

        kafkaConsumer.subscribe(topic, "indexer", new MessageListener<String, Object>() {
            @Override
            public void onMessage(ConsumerRecord<String, Object> data) {
                String value = (String) data.value();
                log.info("From message listener : " + value);
            }
        });

    }

}
