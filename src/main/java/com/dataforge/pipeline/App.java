package com.dataforge.pipeline;

import com.dataforge.pipeline.analysis.DataProfiler;
import com.dataforge.pipeline.clean.OrderCleaner;
import com.dataforge.pipeline.clean.UserCleaner;
import com.dataforge.pipeline.load.HiveLoader;
import com.dataforge.pipeline.model.OrderRecord;
import com.dataforge.pipeline.model.UserRecord;
import com.dataforge.pipeline.util.DataPaths;

import java.nio.file.Path;
import java.util.List;

public final class App {

    public static void main(String[] args) {
        Path projectRoot = Path.of("").toAbsolutePath();
        DataPaths paths = new DataPaths(projectRoot);

        // 可选：先做一次原始数据 profiling，帮助理解字段与分布
        try {
            DataProfiler profiler = new DataProfiler(paths);
            profiler.profileUsers();
            profiler.profileOrders();
        } catch (Exception e) {
            System.err.println("数据 Profiling 失败，不影响后续 ETL: " + e.getMessage());
        }

        UserCleaner userCleaner = new UserCleaner(paths);
        List<UserRecord> users = userCleaner.clean();
        userCleaner.write(users);
        System.out.println("[1/3] 用户数据清洗完成");

        OrderCleaner orderCleaner = new OrderCleaner(paths);
        List<OrderRecord> orders = orderCleaner.clean();
        orderCleaner.write(orders);
        System.out.println("[2/3] 订单数据清洗完成");

        HiveLoader loader = new HiveLoader(paths.hiveConfig());
        loader.loadUsersFromCsv(paths.cleanedUsers());
        loader.loadOrdersFromJson(paths.cleanedOrders());
        System.out.println("[3/3] Hive 加载完成");
    }
}

