package org.egov.kafka.rdb.repository;

import org.egov.kafka.rdb.model.TopicOffsetLog;
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
public class ConsumerTopicOffsetLogsRepository {

    @Autowired
    private TablesUtil tablesUtil;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String INSERT_lOG_QUERY = "INSERT INTO :table_name (timestamp, topic, " +
            "consumer_group_offset) VALUES (?, ?, ?)";

    private static final String GET_LAST_LOG_QUERY = "SELECT * FROM :table_name WHERE topic = ? " +
            "ORDER BY id DESC LIMIT 1";


    public TopicOffsetLog insertLog(String consumerGroupId, TopicOffsetLog topicOffsetLog) {
        tablesUtil.createTableIfNotExist(consumerGroupId, topicOffsetLog.getTopic());

        String consumerOffsetTableName = tablesUtil.getTableNameForConsumerGroupId(consumerGroupId);
        String query = INSERT_lOG_QUERY.replaceAll(":table_name", consumerOffsetTableName);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(query, new String[]{"id"});
                    ps.setLong(1, topicOffsetLog.getTimestamp());
                    ps.setString(2, topicOffsetLog.getTopic());
                    ps.setLong(3, topicOffsetLog.getConsumerGroupOffset());
                    return ps;
                }, keyHolder);
        topicOffsetLog.setId(keyHolder.getKey().longValue());
        return topicOffsetLog;
    }

    public Optional<TopicOffsetLog> getLastLog(String consumerGroupId, String topic) {
        tablesUtil.createTableIfNotExist(consumerGroupId, topic);
        String consumerOffsetTableName = tablesUtil.getTableNameForConsumerGroupId(consumerGroupId);
        String query = GET_LAST_LOG_QUERY.replaceAll(":table_name", consumerOffsetTableName);

        List<TopicOffsetLog> topicOffsetLogList =
                jdbcTemplate.query(query, new Object[] { topic },
                        new BeanPropertyRowMapper<>(TopicOffsetLog.class));

        if(topicOffsetLogList.size() == 1)
            return Optional.ofNullable(topicOffsetLogList.get(0));
        return Optional.empty();
    }

}
