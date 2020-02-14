package org.egov.kafka.rdb.repository;

import org.egov.kafka.rdb.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

@Repository
public class MessageRepository {

    @Autowired
    private TablesUtil tablesUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String INSERT_MESSAGE_QUERY = "INSERT INTO :table_name (timestamp, key, value) VALUES (?, ?, ?)";

    private String READ_NEXT_MESSAGE_AFTER_GIVEN_ID_QUERY = "SELECT * FROM :table_name WHERE id > ? LIMIT 1";

    public Message insertMessage(String topic, Message message) {
        tablesUtil.createTopicTableIfNotExist(topic);
        String topicTableName = tablesUtil.getTableNameForTopic(topic);
        String query = INSERT_MESSAGE_QUERY.replace(":table_name", topicTableName);

        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(query, new String[]{"id"});
                    ps.setLong(1, message.getTimestamp());
                    ps.setString(2, message.getKey());
                    ps.setString(3, message.getValue());
                    return ps;
                }, keyHolder);

        Long messageId = keyHolder.getKey().longValue();

        message.setId(messageId);

        return message;
    }

    public Optional<Message> readMessageAfterOffset(String topic, Long offset) {
        String topicTableName = tablesUtil.getTableNameForTopic(topic);
        String query = READ_NEXT_MESSAGE_AFTER_GIVEN_ID_QUERY.replace(":table_name", topicTableName);
        List<Message> messages = jdbcTemplate.query(query, new Object[] { offset },
                new BeanPropertyRowMapper<>(Message.class));

        if(messages.size() == 1)
            return Optional.ofNullable(messages.get(0));
        return Optional.empty();
    }

}
