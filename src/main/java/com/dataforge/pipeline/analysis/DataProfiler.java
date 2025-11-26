package com.dataforge.pipeline.analysis;

import com.dataforge.pipeline.util.DataPaths;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataProfiler {

    private final DataPaths paths;
    private final ObjectMapper mapper = new ObjectMapper();

    public DataProfiler(DataPaths paths) {
        this.paths = paths;
    }

    public void profileUsers() throws Exception {
        try (CSVReader reader = new CSVReader(Files.newBufferedReader(paths.usersRaw()))) {
            String[] header = reader.readNext();
            if (header == null) {
                System.out.println("users.csv 为空");
                return;
            }
            long rowCount = 0;
            long[] nullCount = new long[header.length];
            @SuppressWarnings("unchecked")
            Set<String>[] uniques = new Set[header.length];
            for (int i = 0; i < header.length; i++) {
                uniques[i] = new HashSet<>();
            }
            String[] row;
            while ((row = reader.readNext()) != null) {
                if (row.length == 0 || (row.length == 1 && row[0].isBlank())) {
                    continue;
                }
                rowCount++;
                for (int i = 0; i < header.length; i++) {
                    String v = i < row.length ? row[i] : null;
                    if (v == null || v.isBlank()) {
                        nullCount[i]++;
                    } else {
                        uniques[i].add(v);
                    }
                }
            }
            System.out.println("=== users.csv 字段统计 ===");
            System.out.println("总行数: " + rowCount);
            for (int i = 0; i < header.length; i++) {
                System.out.printf("字段 %-10s | null=%d | unique=%d%n", header[i], nullCount[i], uniques[i].size());
            }
        }
    }

    public void profileOrders() throws Exception {
        Path path = paths.ordersRaw();
        long rowCount = 0;
        Map<String, Long> nullCount = new HashMap<>();
        Map<String, Set<String>> uniques = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                rowCount++;
                JsonNode node = mapper.readTree(line);
                node.fieldNames().forEachRemaining(f -> {
                    JsonNode v = node.get(f);
                    nullCount.putIfAbsent(f, 0L);
                    uniques.computeIfAbsent(f, k -> new HashSet<>());
                    if (v.isNull() || (v.isTextual() && v.asText().isBlank())) {
                        nullCount.put(f, nullCount.get(f) + 1);
                    } else {
                        uniques.get(f).add(v.asText());
                    }
                });
            }
        }
        System.out.println("=== orders.json 字段统计 ===");
        System.out.println("总行数: " + rowCount);
        for (String field : uniques.keySet()) {
            long n = nullCount.getOrDefault(field, 0L);
            int u = uniques.get(field).size();
            System.out.printf("字段 %-10s | null=%d | unique=%d%n", field, n, u);
        }
    }
}


