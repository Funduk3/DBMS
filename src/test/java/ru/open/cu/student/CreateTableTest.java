package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.open.cu.student.ast.ColumnDef;
import ru.open.cu.student.ast.QueryTree;
import ru.open.cu.student.ast.TypeName;
import ru.open.cu.student.catalog.model.TableDefinition;
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

    @BeforeEach
    void setUp() {
        pageFileManager = new HeapPageFileManager();
        catalogManager = new CatalogManagerImpl(pageFileManager);
        operationManager = new OperationManagerImpl((CatalogManagerImpl) catalogManager, pageFileManager);
        planner = new PlannerImpl(catalogManager);
        optimizer = new OptimizerImpl();
        executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        executionEngine = new QueryExecutionEngineImpl();
    }

    @Test
    void testCreateTableSuccess() {
        TableDefinition createTable = new TableDefinition(
                1, "users", "BASE TABLE", "users_file", 0
        );

        List<ColumnDef> createColumns = new ArrayList<>();
        createColumns.add(new ColumnDef("id", new TypeName("INT64")));
        createColumns.add(new ColumnDef("name", new TypeName("VARCHAR")));

        QueryTree createQuery = new QueryTree(createTable, createColumns, QueryTree.QueryType.CREATE);

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
        TableDefinition createTable = new TableDefinition(
                2, "products2", "BASE TABLE", "products_file", 0
        );

        List<ColumnDef> createColumns = new ArrayList<>();
        createColumns.add(new ColumnDef("id", new TypeName("INT64")));
        createColumns.add(new ColumnDef("price", new TypeName("INT64")));
        createColumns.add(new ColumnDef("name", new TypeName("VARCHAR")));
        createColumns.add(new ColumnDef("available", new TypeName("VARCHAR")));

        QueryTree createQuery = new QueryTree(createTable, createColumns, QueryTree.QueryType.CREATE);

        LogicalPlanNode logicalPlan = planner.plan(createQuery);
        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        Executor executor = executorFactory.createExecutor(physicalPlan);
        executionEngine.execute(executor);

        var table = catalogManager.getTable("products2");
        assertNotNull(table);
        assertEquals(4, table.getColumns().size());
        assertEquals("id", table.getColumns().get(0).getName());
        assertEquals("price", table.getColumns().get(1).getName());
        assertEquals("name", table.getColumns().get(2).getName());
        assertEquals("available", table.getColumns().get(3).getName());
    }

    @Test
    void testCreateTableWithInvalidType() {
        TableDefinition createTable = new TableDefinition(
                3, "invalid_table", "BASE TABLE", "invalid_file", 0
        );

        List<ColumnDef> createColumns = new ArrayList<>();
        createColumns.add(new ColumnDef("id", new TypeName("not_a_real_type")));

        QueryTree createQuery = new QueryTree(createTable, createColumns, QueryTree.QueryType.CREATE);

        assertThrows(Exception.class, () -> planner.plan(createQuery));
    }

    @Test
    void testCreateTableWithoutName() {
        TableDefinition createTable = new TableDefinition(
                4, "", "BASE TABLE", "empty_file", 0
        );

        List<ColumnDef> createColumns = new ArrayList<>();
        createColumns.add(new ColumnDef("id", new TypeName("int4")));

        QueryTree createQuery = new QueryTree(createTable, createColumns, QueryTree.QueryType.CREATE);

        assertThrows(IllegalArgumentException.class, () -> planner.plan(createQuery));
    }

    @Test
    void testCreateTableWithNullQueryTree() {
        assertThrows(IllegalArgumentException.class, () -> planner.plan(null));
    }
}
