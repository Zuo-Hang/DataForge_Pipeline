package com.dataforge.pipeline.config;

public record HiveConfig(String host, int port, String username, String database, String transport) {
    public String jdbcUrl() {
        return String.format("jdbc:hive2://%s:%d/%s", host, port, database);
    }
}

