package com.dataforge.pipeline.model;

import java.time.LocalDate;
import java.util.Objects;

public record UserRecord(long userId, String name, Double age, LocalDate signupDate) {
    public static UserRecord of(long userId, String name, Double age, LocalDate signupDate) {
        return new UserRecord(userId, name, age, signupDate);
    }

    public UserRecord withAge(Double newAge) {
        return new UserRecord(userId, name, newAge, signupDate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserRecord that = (UserRecord) o;
        return userId == that.userId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId);
    }
}

