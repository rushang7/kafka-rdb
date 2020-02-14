package org.egov.kafka.rdb.model;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TopicOffsetLog {

    private Long id;

    private Long timestamp;

    private String topic;

    private Long consumerGroupOffset;
}
