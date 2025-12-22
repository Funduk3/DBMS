package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.execution.executors.Executor;
import ru.open.cu.student.execution.ExecutorFactory;
import ru.open.cu.student.execution.ExecutorFactoryImpl;
import ru.open.cu.student.execution.QueryExecutionEngine;
import ru.open.cu.student.execution.QueryExecutionEngineImpl;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;
import ru.open.cu.student.memory.replacer.Replacer;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.parser.nodes.AConst;
import ru.open.cu.student.parser.nodes.AExpr;
import ru.open.cu.student.parser.nodes.ColumnRef;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.semantic.QueryTree;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SelectTableTest {

    private CatalogManager catalogManager;
    private OperationManager operationManager;
    private Planner planner;
    private Optimizer optimizer;
    private ExecutorFactory executorFactory;
    private QueryExecutionEngine executionEngine;
    private PageFileManager pageFileManager;
    private DefaultBufferPoolManager bufferPoolManager;
    private Replacer replacer;

    @BeforeEach
    void setUp() {
        replacer = new ClockReplacer(10);
        pageFileManager = new HeapPageFileManager();
        bufferPoolManager = new DefaultBufferPoolManager(10, pageFileManager, replacer);
        catalogManager = new CatalogManagerImpl(bufferPoolManager, pageFileManager);
        operationManager = new OperationManagerImpl((CatalogManagerImpl) catalogManager, pageFileManager);
        planner = new PlannerImpl(catalogManager);
        optimizer = new OptimizerImpl();
        executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        executionEngine = new QueryExecutionEngineImpl();
    }

    @Test
    void testSelectAllFromUsers() {
        TableDefinition createTable = new TableDefinition(10, "users", "BASE TABLE", "users_file", 0);
        List<ColumnDefinition> colsForCatalog = new ArrayList<>();
        colsForCatalog.add(new ColumnDefinition(100, 10, 1, "id", 0));
        colsForCatalog.add(new ColumnDefinition(101, 10, 2, "name", 1));
        createTable.setColumns(colsForCatalog);

        List<ColumnDefinition> targetColumns = new ArrayList<>();
        List<TableDefinition> fromTables = new ArrayList<>();
        fromTables.add(createTable);
        AExpr filter = null;

        QueryTree selectQuery = new QueryTree(targetColumns, fromTables, filter);

        LogicalPlanNode logicalPlan = planner.plan(selectQuery);
        assertNotNull(logicalPlan);
        assertEquals("Scan", logicalPlan.getNodeType());

        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        assertNotNull(physicalPlan);
        assertEquals("PhysicalSeqScan", physicalPlan.getNodeType());

        Executor executor = executorFactory.createExecutor(physicalPlan);
        List<Object> results = executionEngine.execute(executor);
        assertNotNull(results);
    }

    @Test
    void testSelectProjectionWithWhere() {
        TableDefinition createTable = new TableDefinition(11, "users", "BASE TABLE", "users_file2", 0);
        List<ColumnDefinition> colsForCatalog = new ArrayList<>();
        colsForCatalog.add(new ColumnDefinition(200, 11, 1, "id", 0));
        colsForCatalog.add(new ColumnDefinition(201, 11, 2, "name", 1));
        createTable.setColumns(colsForCatalog);

        List<ColumnDefinition> targetColumns = new ArrayList<>();
        targetColumns.add(new ColumnDefinition(200, 11, 1, "id", 0));
        targetColumns.add(new ColumnDefinition(201, 11, 2, "name", 1));

        List<TableDefinition> fromTables = new ArrayList<>();
        fromTables.add(createTable);

        AExpr where = new AExpr("=", new AConst(1), new ColumnRef("users", "id"));

        QueryTree selectQuery = new QueryTree(targetColumns, fromTables, where);

        LogicalPlanNode logicalPlan = planner.plan(selectQuery);
        assertNotNull(logicalPlan);
        assertNotNull(logicalPlan.getNodeType());

        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        assertNotNull(physicalPlan);

        Executor executor = executorFactory.createExecutor(physicalPlan);
        List<Object> results = executionEngine.execute(executor);
        assertNotNull(results);
    }

    @Test
    void testSelectFromNonexistentTable() {
        TableDefinition ghost = new TableDefinition(999, "ghost_table", "BASE TABLE", "ghost_file", 0);
        List<ColumnDefinition> targets = new ArrayList<>();
        List<TableDefinition> from = new ArrayList<>();
        from.add(ghost);

        QueryTree q = new QueryTree(targets, from, null);

        assertThrows(Exception.class, () -> planner.plan(q));
    }
}
