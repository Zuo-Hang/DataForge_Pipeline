package com.dataforge.pipeline.clean;

import com.dataforge.pipeline.model.UserRecord;
import com.dataforge.pipeline.util.DataPaths;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UserCleaner {

    private final DataPaths paths;

    public UserCleaner(DataPaths paths) {
        this.paths = paths;
    }

    public List<UserRecord> clean() {
        List<UserRecord> raw = readCsv(paths.usersRaw());
        Map<Long, UserRecord> deduped = new LinkedHashMap<>();
        for (UserRecord record : raw) {
            deduped.putIfAbsent(record.userId(), record);
        }
        List<UserRecord> cleaned = new ArrayList<>(deduped.values());
        Double median = medianAge(cleaned);
        return cleaned.stream()
                .map(user -> user.age() == null ? user.withAge(median) : user)
                .sorted(Comparator.comparingLong(UserRecord::userId))
                .toList();
    }

    public void write(List<UserRecord> records) {
        try (Writer writer = Files.newBufferedWriter(paths.cleanedUsers());
             CSVWriter csvWriter = new CSVWriter(writer)) {
            csvWriter.writeNext(new String[]{"user_id", "name", "age", "signup_dt"});
            for (UserRecord record : records) {
                csvWriter.writeNext(new String[]{
                        String.valueOf(record.userId()),
                        record.name(),
                        record.age() == null ? "" : record.age().toString(),
                        record.signupDate().toString()
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("写入清洗后的用户文件失败", e);
        }
    }

    private List<UserRecord> readCsv(Path path) {
        try (Reader reader = Files.newBufferedReader(path); CSVReader csvReader = new CSVReader(reader)) {
            List<String[]> rows = csvReader.readAll();
            List<UserRecord> result = new ArrayList<>();
            for (int i = 1; i < rows.size(); i++) {
                String[] row = rows.get(i);
                if (row.length == 0 || row[0].isBlank()) {
                    continue;
                }
                long userId = Long.parseLong(row[0]);
                String name = row.length > 1 ? row[1] : "";
                Double age = parseDouble(row, 2);
                LocalDate signup = row.length > 3 && !row[3].isBlank() ? LocalDate.parse(row[3]) : LocalDate.now();
                result.add(UserRecord.of(userId, name, age, signup));
            }
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("读取用户 CSV 失败: " + path, e);
        }
    }

    private Double parseDouble(String[] row, int index) {
        if (row.length <= index || row[index].isBlank()) {
            return null;
        }
        return Double.parseDouble(row[index]);
    }

    private Double medianAge(List<UserRecord> users) {
        List<Double> ages = users.stream()
                .map(UserRecord::age)
                .filter(v -> v != null && !Double.isNaN(v))
                .sorted()
                .toList();
        if (ages.isEmpty()) {
            return null;
        }
        int size = ages.size();
        return size % 2 == 1
                ? ages.get(size / 2)
                : (ages.get(size / 2 - 1) + ages.get(size / 2)) / 2.0;
    }
}

