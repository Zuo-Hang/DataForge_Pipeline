package com.dataforge.pipeline.load;

import com.dataforge.pipeline.config.HiveConfig;
import com.dataforge.pipeline.util.ConfigLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class HiveLoader {

    private final HiveConfig config;
    private final ObjectMapper mapper = new ObjectMapper();

    public HiveLoader(Path configPath) {
        this.config = ConfigLoader.loadHive(configPath);
    }

    public void loadUsersFromCsv(Path csvPath) {
        String sql = "INSERT INTO TABLE users_etl (user_id, name, age, signup_dt) VALUES (?, ?, ?, ?)";
        try (Connection connection = connect();
             PreparedStatement ps = connection.prepareStatement(sql);
             CSVReader reader = new CSVReader(Files.newBufferedReader(csvPath))) {
            String[] row;
            reader.readNext(); // skip header
            while ((row = reader.readNext()) != null) {
                ps.setLong(1, Long.parseLong(row[0]));
                ps.setString(2, row[1]);
                if (row[2] == null || row[2].isBlank()) {
                    ps.setNull(3, java.sql.Types.DOUBLE);
                } else {
                    ps.setDouble(3, Double.parseDouble(row[2]));
                }
                ps.setDate(4, java.sql.Date.valueOf(row[3]));
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            throw new IllegalStateException("写入 Hive users_etl 失败", e);
        }
    }

    public void loadOrdersFromJson(Path jsonPath) {
        String sql = "INSERT INTO TABLE orders_etl (order_id, user_id, amount, order_time, status, dt) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection connection = connect();
             PreparedStatement ps = connection.prepareStatement(sql);
             BufferedReader reader = Files.newBufferedReader(jsonPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = mapper.readTree(line);
                ps.setString(1, node.get("orderId").asText());
                ps.setLong(2, node.get("userId").asLong());
                ps.setDouble(3, node.get("amount").asDouble());
                ps.setTimestamp(4, Timestamp.from(java.time.Instant.parse(node.get("orderTime").asText())));
                ps.setString(5, node.get("status").asText());
                ps.setString(6, node.get("dt").asText());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            throw new IllegalStateException("写入 Hive orders_etl 失败", e);
        }
    }

    private Connection connect() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection(config.jdbcUrl(), config.username(), "");
    }
}

