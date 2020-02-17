package org.egov.kafka.rdb.client;

import lombok.extern.slf4j.Slf4j;
import org.egov.kafka.rdb.service.ConsumerService;
import org.egov.kafka.rdb.util.ConsumerJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@Slf4j
@Component
public class KafkaConsumer {

    @Value("${consumer.poll.time.ms}")
    private Long consumerPollTime;

    @Autowired
    private ConsumerService consumerService;

    private Scheduler scheduler;

    @PostConstruct
    public void init() throws SchedulerException {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        scheduler = schedulerFactory.getScheduler();
        scheduler.start();
    }

    public void subscribe(String topic, String consumerGroupId, MessageListener<String, Object> messageListener)
            throws SchedulerException {

        if(consumerService != null) {
            scheduler.getContext().put("consumerService", consumerService);
            log.info("Consumer Service added to scheduler");
        }

        scheduler.getContext().put(topic + consumerGroupId, messageListener);
        JobDetail jobDetail = JobBuilder.newJob(ConsumerJob.class).usingJobData("topic", topic)
                .usingJobData("consumerGroupId", consumerGroupId)
                .build();

        Trigger trigger = newTrigger()
                .withIdentity(topic + consumerGroupId, "consumer")
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInMilliseconds(consumerPollTime)
                        .repeatForever())
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

}
