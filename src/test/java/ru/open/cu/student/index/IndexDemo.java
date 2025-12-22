package ru.open.cu.student.index;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.execution.executors.Executor;
import ru.open.cu.student.execution.executors.BTreeIndexScanExecutor;
import ru.open.cu.student.execution.executors.HashIndexScanExecutor;
import ru.open.cu.student.index.btree.BPlusTreeIndex;
import ru.open.cu.student.index.btree.BPlusTreeIndexImpl;
import ru.open.cu.student.index.hash.HashIndex;
import ru.open.cu.student.index.hash.HashIndexImpl;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class IndexDemo {

    public static void main(String[] args) {
        System.out.println("=== Index Demo (B+-Tree и Hash) ===\n");

        try {
            cleanupTestData();

            PageFileManager pageManager = new HeapPageFileManager();
            CatalogManagerImpl catalogManager = new CatalogManagerImpl(
                    new DefaultBufferPoolManager(16, new HeapPageFileManager(), new ClockReplacer(16)),
                    new HeapPageFileManager()
            );
            OperationManager operationManager = new OperationManagerImpl(
                    catalogManager,
                    pageManager
            );

            TableDefinition ordersTable = createOrdersTable(catalogManager);

            Path btreeIndexPath = Paths.get("data", "btree_index_demo");
            Path hashIndexPath = Paths.get("data", "hash_index_demo");
            Files.createDirectories(btreeIndexPath.getParent());
            Files.createDirectories(hashIndexPath.getParent());

            BPlusTreeIndex btreeIndex = new BPlusTreeIndexImpl(
                    "idx_orders_date",
                    "order_date",
                    4,
                    pageManager,
                    btreeIndexPath
            );

            HashIndex hashIndex = new HashIndexImpl(
                    "idx_orders_id",
                    "order_id",
                    pageManager,
                    operationManager,
                    hashIndexPath
            );

            catalogManager.registerIndex(btreeIndex);
            catalogManager.registerIndex(hashIndex);

            System.out.println("=== Генерация тестовых данных ===");
            generateTestData(ordersTable, operationManager, btreeIndex, hashIndex);

            System.out.println("\n=== Демонстрация B+Tree индекса ===");
            demonstrateBTreeOperations(btreeIndex, operationManager, ordersTable);

            System.out.println("\n=== Демонстрация Hash индекса ===");
            demonstrateHashOperations(hashIndex, operationManager, ordersTable);

            System.out.println("\nДемонстрация завершена успешно!");

        } catch (Exception e) {
            System.err.println("Ошибка во время демонстрации: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void cleanupTestData() throws IOException {
        Path dataDir = Paths.get("data");
        if (Files.exists(dataDir)) {
            try (var paths = Files.walk(dataDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }

        Path catalogDir = Paths.get("catalog");
        if (Files.exists(catalogDir)) {
            try (var paths = Files.walk(catalogDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException ignored) {
                            }
                        });
            }
        }
    }

    private static TableDefinition createOrdersTable(CatalogManager catalogManager) {
        TypeDefinition intType = catalogManager.getTypeByName("INT64");
        int intOid = intType.getOid();

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(new ColumnDefinition(1, 0, intOid, "order_id", 1));
        columns.add(new ColumnDefinition(2, 0, intOid, "order_date", 2));
        columns.add(new ColumnDefinition(3, 0, intOid, "amount", 3));

        try {
            return catalogManager.createTable("orders", columns);
        } catch (Exception e) {
            try {
                return catalogManager.getTable("orders");
            } catch (Exception ex) {
                throw new RuntimeException("Не удалось создать или получить таблицу 'orders'", ex);
            }
        }
    }

    private static void generateTestData(TableDefinition ordersTable,
                                         OperationManager operationManager,
                                         BPlusTreeIndex btreeIndex,
                                         HashIndex hashIndex) {
        Random random = new Random(42);

        System.out.println("Генерация данных для таблицы orders:");
        int startDate = 20240101;

        for (int i = 1; i <= 20; i++) {
            long orderId = 1000 + i;
            int orderDate = startDate + (i - 1) * 2;
            long amount = 100 + random.nextInt(9001);

            try {
                operationManager.insert("orders", orderId, (long) orderDate, amount);

                TID tid = new TID(0, (short) (i - 1));
                btreeIndex.insert(orderDate, tid);
                hashIndex.insert((int) orderId, tid);

                System.out.println(String.format(
                        "  Вставлен заказ: order_id=%d, date=%d, amount=%d, tid=%s",
                        orderId, orderDate, amount, tid
                ));
            } catch (Exception e) {
                System.err.println("Ошибка при вставке заказа #" + i + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("\nСтатистика B+Tree индекса:");
        System.out.println("  Высота дерева: " + btreeIndex.getHeight());
        System.out.println("  Порядок (M): " + btreeIndex.getOrder());
        System.out.println("  Всего записей: " + btreeIndex.scanAll().size());

        System.out.println("\nСтатистика Hash индекса:");
        System.out.println("  Количество бакетов: " + hashIndex.getNumBuckets());
        System.out.println("  Максимальный бакет: " + hashIndex.getMaxBucket());
        System.out.println("  Всего записей: " + hashIndex.getRecordCount());
    }

    private static void demonstrateBTreeOperations(BPlusTreeIndex btreeIndex,
                                                   OperationManager operationManager,
                                                   TableDefinition ordersTable) {
        System.out.println("\n1. Поиск по точному совпадению (order_date = 20240110):");
        List<TID> exactResults = btreeIndex.search(20240110);
        System.out.println("   Найдено записей: " + exactResults.size());
        for (TID tid : exactResults) {
            System.out.println("   TID: " + tid);
        }

        System.out.println("\n2. Range поиск [20240105, 20240115]:");
        List<TID> rangeResults = btreeIndex.rangeSearch(20240105, 20240115, true);
        System.out.println("   Найдено записей: " + rangeResults.size());
        for (int i = 0; i < rangeResults.size(); i++) {
            System.out.println(String.format("   #%d: TID=%s", i + 1, rangeResults.get(i)));
        }

        System.out.println("\n3. BTreeIndexScanExecutor:");
        Executor executor = new BTreeIndexScanExecutor(
                operationManager,
                btreeIndex,
                20240105,
                20240115,
                ordersTable
        );

        executor.open();
        Object result;
        int count = 0;
        while ((result = executor.next()) != null) {
            if (result instanceof List<?> row && row.size() >= 3) {
                long orderId = convertToLong(row.get(0));
                long orderDate = convertToLong(row.get(1));
                long amount = convertToLong(row.get(2));

                System.out.println(String.format(
                        "   Заказ #%d: order_id=%d, date=%d, amount=%d",
                        ++count, orderId, orderDate, amount
                ));
            }
        }
        executor.close();

        System.out.println("\n4. Поиск записей с датой >= 20240120:");
        List<TID> greaterResults = btreeIndex.searchGreaterThan(20240120, true);
        System.out.println("   Найдено записей: " + greaterResults.size());
        for (int i = 0; i < Math.min(5, greaterResults.size()); i++) {
            System.out.println(String.format("   #%d: TID=%s", i + 1, greaterResults.get(i)));
        }
    }

    private static void demonstrateHashOperations(HashIndex hashIndex,
                                                  OperationManager operationManager,
                                                  TableDefinition ordersTable) {
        System.out.println("\n1. Поиск по точному совпадению (order_id = 1005):");
        List<TID> exactResults = hashIndex.search(1005);
        System.out.println("   Найдено записей: " + exactResults.size());
        for (TID tid : exactResults) {
            System.out.println("   TID: " + tid);
        }

        System.out.println("\n2. Поиск нескольких заказов:");
        int[] orderIds = {1001, 1010, 1015, 1020};
        for (int orderId : orderIds) {
            List<TID> results = hashIndex.search(orderId);
            System.out.println(String.format(
                    "   order_id=%d: найдено %d записей",
                    orderId, results.size()
            ));
            if (!results.isEmpty()) {
                System.out.println("     TID: " + results.get(0));
            }
        }

        System.out.println("\n3. HashIndexScanExecutor:");
        Executor executor = new HashIndexScanExecutor(
                operationManager,
                hashIndex,
                1012,
                ordersTable
        );

        executor.open();
        Object result;
        int count = 0;
        while ((result = executor.next()) != null) {
            if (result instanceof List<?> row && row.size() >= 3) {
                long orderId = convertToLong(row.get(0));
                long orderDate = convertToLong(row.get(1));
                long amount = convertToLong(row.get(2));

                System.out.println(String.format(
                        "   Заказ #%d: order_id=%d, date=%d, amount=%d",
                        ++count, orderId, orderDate, amount
                ));
            }
        }
        executor.close();

        System.out.println("\n4. Полное сканирование индекса:");
        List<TID> allResults = hashIndex.scanAll();
        System.out.println("   Всего записей в индексе: " + allResults.size());
        for (int i = 0; i < Math.min(5, allResults.size()); i++) {
            System.out.println(String.format("   #%d: TID=%s", i + 1, allResults.get(i)));
        }
    }

    private static long convertToLong(Object obj) {
        if (obj instanceof Number n) {
            return n.longValue();
        }
        if (obj instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0;
    }
}
