package com.dataforge.pipeline.util;

import java.nio.file.Path;

public final class DataPaths {

    private final Path projectRoot;

    public DataPaths(Path projectRoot) {
        this.projectRoot = projectRoot;
    }

    public Path usersRaw() {
        return projectRoot.resolve("data_sources/users.csv");
    }

    public Path ordersRaw() {
        return projectRoot.resolve("data_sources/orders.json");
    }

    public Path cleanedUsers() {
        return projectRoot.resolve("data_sources/cleaned_users.csv");
    }

    public Path cleanedOrders() {
        return projectRoot.resolve("data_sources/cleaned_orders.json");
    }

    public Path hiveConfig() {
        return projectRoot.resolve("config/hive_config.yaml");
    }
}

