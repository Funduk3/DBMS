package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;
import ru.open.cu.student.memory.replacer.Replacer;
import ru.open.cu.student.semantic.QueryTree;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.execution.ExecutorFactory;
import ru.open.cu.student.execution.ExecutorFactoryImpl;
import ru.open.cu.student.execution.QueryExecutionEngine;
import ru.open.cu.student.execution.QueryExecutionEngineImpl;
import ru.open.cu.student.execution.executors.Executor;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.node.LogicalPlanNode;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CreateTableTest {

    private CatalogManager catalogManager;
    private OperationManager operationManager;
    private Planner planner;
    private Optimizer optimizer;
    private ExecutorFactory executorFactory;
    private QueryExecutionEngine executionEngine;
    private PageFileManager pageFileManager;
    private Replacer replacer;
    private DefaultBufferPoolManager bufferPoolManager;

    @BeforeEach
    void setUp() {
        replacer = new ClockReplacer(10);
        pageFileManager = new HeapPageFileManager();
        bufferPoolManager = new DefaultBufferPoolManager(10, pageFileManager, replacer);
        catalogManager = new CatalogManagerImpl(bufferPoolManager, pageFileManager);
        operationManager = new OperationManagerImpl((CatalogManagerImpl) catalogManager, pageFileManager);
        planner = new PlannerImpl(catalogManager);
        optimizer = new OptimizerImpl(catalogManager);
        executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        executionEngine = new QueryExecutionEngineImpl();
    }

    @Test
    void testCreateTableSuccess() {
        String tableName = "users";

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(new ColumnDefinition(0, "id", catalogManager.getTypeByName("INT64").getOid()));
        columns.add(new ColumnDefinition(1, "name", catalogManager.getTypeByName("VARCHAR").getOid()));

        QueryTree createQuery = new QueryTree(tableName, columns, QueryTree.QueryType.CREATE);

        LogicalPlanNode logicalPlan = planner.plan(createQuery);
        assertNotNull(logicalPlan);
        assertEquals("CreateTable", logicalPlan.getNodeType());

        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        assertNotNull(physicalPlan);
        assertEquals("PhysicalCreate", physicalPlan.getNodeType());

        Executor executor = executorFactory.createExecutor(physicalPlan);
        List<Object> results = executionEngine.execute(executor);

        assertNotNull(catalogManager.getTable("users"));
        assertEquals("users", catalogManager.getTable("users").getName());

        assertTrue(results.isEmpty() || results.stream().allMatch(r -> r == null));
    }

    @Test
    void testCreateTableWithMultipleTypes() {
        String tableName = "products1";

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(new ColumnDefinition(0, "id", catalogManager.getTypeByName("INT64").getOid()));
        columns.add(new ColumnDefinition(1, "price", catalogManager.getTypeByName("INT64").getOid()));
        columns.add(new ColumnDefinition(2, "name", catalogManager.getTypeByName("VARCHAR").getOid()));
        columns.add(new ColumnDefinition(3, "available", catalogManager.getTypeByName("VARCHAR").getOid()));

        QueryTree createQuery = new QueryTree(tableName, columns, QueryTree.QueryType.CREATE);

        LogicalPlanNode logicalPlan = planner.plan(createQuery);
        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        Executor executor = executorFactory.createExecutor(physicalPlan);
        executionEngine.execute(executor);

        var table = catalogManager.getTable("products1");
        assertNotNull(table);
        assertEquals(4, table.getColumns().size());
        assertEquals("id", table.getColumns().get(0).getName());
        assertEquals("price", table.getColumns().get(1).getName());
        assertEquals("name", table.getColumns().get(2).getName());
        assertEquals("available", table.getColumns().get(3).getName());
    }

    @Test
    void testCreateTableWithInvalidType() {
        String tableName = "invalid_table";

        // Пытаемся создать колонку с несуществующим типом
        // Это должно вызвать исключение при получении типа
        assertThrows(Exception.class, () -> {
            List<ColumnDefinition> columns = new ArrayList<>();
            columns.add(new ColumnDefinition(0, "id", catalogManager.getTypeByName("not_a_real_type").getOid()));

            QueryTree createQuery = new QueryTree(tableName, columns, QueryTree.QueryType.CREATE);
            planner.plan(createQuery);
        });
    }

    @Test
    void testCreateTableWithoutName() {
        String tableName = "";

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(new ColumnDefinition(0, "id", catalogManager.getTypeByName("INT64").getOid()));

        QueryTree createQuery = new QueryTree(tableName, columns, QueryTree.QueryType.CREATE);

        assertThrows(IllegalArgumentException.class, () -> planner.plan(createQuery));
    }

    @Test
    void testCreateTableWithNullQueryTree() {
        assertThrows(IllegalArgumentException.class, () -> planner.plan(null));
    }
}