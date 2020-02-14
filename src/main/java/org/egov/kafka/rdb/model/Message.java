package org.egov.kafka.rdb.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Message {

    private Long id;

    private Long timestamp;

    private String key;

    private String value;
}
