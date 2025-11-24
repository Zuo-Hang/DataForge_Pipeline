package com.dataforge.pipeline;

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

