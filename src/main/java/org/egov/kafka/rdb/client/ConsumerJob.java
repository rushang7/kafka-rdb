package org.egov.kafka.rdb.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.kafka.rdb.model.Message;
import org.egov.kafka.rdb.service.ConsumerService;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class ConsumerJob implements Job {

    @Autowired
    private ConsumerService consumerService;

    @SneakyThrows
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        String topic = jobDataMap.getString("topic");
        String consumerGroupId = jobDataMap.getString("consumerGroupId");
        MessageListener<String, Object> messageListener =
                (MessageListener) jobExecutionContext.getScheduler().getContext().get(topic + consumerGroupId);
        ConsumerService consumerService = (ConsumerService) jobExecutionContext.getScheduler().getContext().get("consumerService");

        if(consumerService == null) {
            log.info("ConsumerService empty");
            if(messageListener == null) {
                log.error("null");
            } else {
                log.info("Not null");
            }
            return;
        }


        int messagesConsumed = 0;
        while (true) {
            Optional<Message> optionalMessage = consumerService.getNextMessageFor(topic, consumerGroupId);
            if(optionalMessage.isPresent()) {
                Message message = optionalMessage.get();
                ConsumerRecord consumerRecord = new ConsumerRecord(topic, 0, message.getId(), message.getKey(),
                        message.getValue());
                messageListener.onMessage(consumerRecord);
            } else {
                log.info("Number of consumed messages : ", messagesConsumed);
                break;
            }
        }
    }

}
