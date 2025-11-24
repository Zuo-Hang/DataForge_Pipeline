package com.dataforge.pipeline.util;

import com.dataforge.pipeline.config.HiveConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class ConfigLoader {

    private ConfigLoader() {
    }

    @SuppressWarnings("unchecked")
    public static HiveConfig loadHive(Path configPath) {
        Yaml yaml = new Yaml();
        try (InputStream is = Files.newInputStream(configPath)) {
            Map<String, Object> map = yaml.load(is);
            return new HiveConfig(
                    map.getOrDefault("host", "localhost").toString(),
                    ((Number) map.getOrDefault("port", 10000)).intValue(),
                    map.getOrDefault("username", "hive").toString(),
                    map.getOrDefault("database", "default").toString(),
                    map.getOrDefault("transport", "socket").toString()
            );
        } catch (IOException e) {
            throw new IllegalStateException("无法读取 Hive 配置文件: " + configPath, e);
        }
    }
}

