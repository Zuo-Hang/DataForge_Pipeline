package com.dataforge.pipeline.model;

import java.time.Instant;
import java.time.LocalDate;

public record OrderRecord(String orderId, long userId, double amount, Instant orderTime, String status,
                          LocalDate dt) {
}

