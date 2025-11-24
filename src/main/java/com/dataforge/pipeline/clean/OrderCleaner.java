package com.dataforge.pipeline.clean;

import com.dataforge.pipeline.model.OrderRecord;
import com.dataforge.pipeline.util.DataPaths;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class OrderCleaner {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderCleaner.class);
    private final DataPaths paths;
    private final ObjectMapper mapper;

    public OrderCleaner(DataPaths paths) {
        this.paths = paths;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, true);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.mapper = objectMapper;
    }

    public List<OrderRecord> clean() {
        List<OrderRecord> cleaned = new ArrayList<>();
        for (OrderRaw raw : readNdJson()) {
            if (!isValid(raw)) {
                LOGGER.warn("跳过无效订单记录: {}", raw);
                continue;
            }
            if (raw.amount < 0) {
                LOGGER.debug("过滤负金额订单: {}", raw.orderId);
                continue;
            }
            try {
                Instant timestamp = parseInstant(raw.orderTime);
                cleaned.add(new OrderRecord(
                        raw.orderId,
                        raw.userId,
                        raw.amount,
                        timestamp,
                        normalizeStatus(raw.status),
                        timestamp.atZone(ZoneOffset.UTC).toLocalDate()
                ));
            } catch (DateTimeParseException ex) {
                LOGGER.warn("无法解析订单时间，跳过记录: {}", raw.orderId, ex);
            }
        }
        return cleaned;
    }

    public void write(List<OrderRecord> orders) {
        ObjectWriter writer = mapper.writer();
        try (BufferedWriter bw = Files.newBufferedWriter(paths.cleanedOrders())) {
            for (OrderRecord order : orders) {
                bw.write(writer.writeValueAsString(order));
                bw.newLine();
            }
        } catch (IOException e) {
            throw new IllegalStateException("写入订单 JSON 失败", e);
        }
    }

    private List<OrderRaw> readNdJson() {
        List<OrderRaw> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(paths.ordersRaw())) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                OrderRaw raw = mapper.readValue(line, OrderRaw.class);
                result.add(raw);
            }
        } catch (IOException e) {
            throw new IllegalStateException("读取订单 JSON 失败", e);
        }
        return result;
    }

    private boolean isValid(OrderRaw raw) {
        return raw.orderId != null
                && raw.orderTime != null
                && !raw.orderId.isBlank()
                && !raw.orderTime.isBlank();
    }

    private String normalizeStatus(String status) {
        if (status == null) {
            return "unknown";
        }
        return switch (status.toLowerCase()) {
            case "paid", "pending", "refunded" -> status.toLowerCase();
            default -> "unknown";
        };
    }

    private Instant parseInstant(String value) {
        if (value.endsWith("Z") || value.length() > 19) {
            return Instant.parse(value);
        }
        return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC);
    }

    private static final class OrderRaw {
        public String orderId;
        public long userId;
        public double amount;
        public String orderTime;
        public String status;

        @Override
        public String toString() {
            return "OrderRaw{" +
                    "orderId='" + orderId + '\'' +
                    ", userId=" + userId +
                    ", amount=" + amount +
                    ", orderTime='" + orderTime + '\'' +
                    ", status='" + status + '\'' +
                    '}';
        }
    }
}

