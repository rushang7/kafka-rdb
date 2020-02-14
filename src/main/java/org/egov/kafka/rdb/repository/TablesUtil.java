package org.egov.kafka.rdb.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Repository
public class TablesUtil {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String consumerGroupOffsetTableNamePrefix = "__consumer_offsets_";
    private String topicTableNamePrefix = "__topic_";

    private static final String CREATE_TOPIC_QUERY =
                    "CREATE TABLE :table_name (\n" +
                        "id SERIAL PRIMARY KEY,\n" +
                        "timestamp BIGINT,\n" +
                        "key TEXT,\n" +
                        "value TEXT\n" +
                    ");";

    private static final String CREATE_OFFSETS_TABLE_QUERY =
                    "CREATE TABLE :table_name (\n" +
                        "id SERIAL PRIMARY KEY,\n" +
                        "timestamp BIGINT,\n" +
                        "topic VARCHAR(1000),\n" +
                        "consumer_group_offset BIGINT\n" +
                    ");";

    private static final String CREATE_INDEX_CONSUMER_GROUP_TOPIC =
            "CREATE INDEX ON :table_name(topic);";

    private boolean createTopicTable(String topic) {
        // Create Table query does not support passing params in prepared statement
        String createTableQuery = CREATE_TOPIC_QUERY.replaceAll(":table_name", topic);
        return createTable(createTableQuery);
    }

    private boolean createConsumerOffsetTable(String table) {
        // Create Table query does not support passing params in prepared statement
        try {
            String createTableQuery = CREATE_OFFSETS_TABLE_QUERY.replaceAll(":table_name", table);
            String createIndexQuery = CREATE_INDEX_CONSUMER_GROUP_TOPIC.replaceAll(":table_name", table);
            boolean createTable = createTable(createTableQuery);
            boolean createIndex = createIndexForConsumerOffsetTable(createIndexQuery);
            return createTable && createIndex;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean createTable(String createTableQuery) {
        try {
            jdbcTemplate.update(createTableQuery);
            return true;
        } catch (Exception e) {
            log.error("Create table error ", e);
            return false;
        }
    }

    private boolean createIndexForConsumerOffsetTable(String query) {
        try {
            jdbcTemplate.update(query);
            return true;
        } catch (Exception e) {
            log.error("Create Index Error", e);
            return false;
        }
    }

    public boolean checkIfTableExist(String table) {
        try {
            String[] types = {"TABLE"};
            Connection connection = jdbcTemplate.getDataSource().getConnection();
            ResultSet resultSet = connection.getMetaData().getTables(null, null, "%", types);

            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(3));
            }

            if(tableNames.indexOf(table) != -1) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public String getTableNameForConsumerGroupId(String consumerGroupId) {
        consumerGroupId = consumerGroupId.toLowerCase();
        String translatedConsumerGroupId = consumerGroupOffsetTableNamePrefix + consumerGroupId;
        translatedConsumerGroupId = translatedConsumerGroupId.replaceAll("\\.", "_");
        translatedConsumerGroupId = translatedConsumerGroupId.replaceAll("-", "_");
        return translatedConsumerGroupId;
    }

    public String getTableNameForTopic(String topic) {
        topic = topic.toLowerCase();
        String translatedTopicName = topicTableNamePrefix + topic;
        translatedTopicName = translatedTopicName.replaceAll("\\.", "_");
        translatedTopicName = translatedTopicName.replaceAll("-", "_");
        return translatedTopicName;
    }

    public boolean createTableIfNotExist(String consumerGroupId, String topic) {
        String consumerOffsetTableName = getTableNameForConsumerGroupId(consumerGroupId);
        if(! checkIfTableExist(consumerOffsetTableName)) {
            createConsumerOffsetTable(consumerOffsetTableName);
        }
        createTopicTableIfNotExist(topic);
        return true;
    }

    public boolean createTopicTableIfNotExist(String topic) {
        String topicTableName = getTableNameForTopic(topic);
        if(! checkIfTableExist(topicTableName)) {
            createTopicTable(topicTableName);
        }
        return true;
    }

}
