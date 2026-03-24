package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.memory.buffer.BufferPoolManager;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;
import ru.open.cu.student.memory.replacer.Replacer;
import ru.open.cu.student.parser.nodes.AConst;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.execution.ExecutorFactory;
import ru.open.cu.student.execution.ExecutorFactoryImpl;
import ru.open.cu.student.execution.QueryExecutionEngine;
import ru.open.cu.student.execution.QueryExecutionEngineImpl;
import ru.open.cu.student.execution.executors.Executor;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.semantic.QueryTree;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InsertTableTest {

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
        optimizer = new OptimizerImpl(catalogManager);
        executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        executionEngine = new QueryExecutionEngineImpl();
    }

    @Test
    void testInsertSuccess() {
        List<Object> insertValues = new ArrayList<>();
        insertValues.add(new AConst(1));
        insertValues.add(new AConst("Alice"));
        TableDefinition table = catalogManager.getTable("users");

        QueryTree insertQuery = new QueryTree(table, insertValues, QueryTree.QueryType.INSERT);

        LogicalPlanNode insertLogical = planner.plan(insertQuery);
        assertNotNull(insertLogical);
        assertEquals("Insert", insertLogical.getNodeType());

        PhysicalPlanNode insertPhysical = optimizer.optimize(insertLogical);
        assertNotNull(insertPhysical);
        assertEquals("PhysicalInsert", insertPhysical.getNodeType());

        Executor insertExecutor = executorFactory.createExecutor(insertPhysical);
        List<Object> results = executionEngine.execute(insertExecutor);

        assertNotNull(catalogManager.getTable("users"));
        assertEquals("users", catalogManager.getTable("users").getName());

        assertTrue(results.isEmpty() || results.stream().allMatch(r -> r == null));
    }

    @Test
    void testInsertIntoNonexistentTable() {
        TableDefinition missingTable = new TableDefinition(99, "ghost", "BASE TABLE", "ghost_file", 0);

        List<Object> insertValues = new ArrayList<>();
        insertValues.add(new AConst(1));
        insertValues.add(new AConst("Nobody"));

        QueryTree insertQuery = new QueryTree(missingTable, insertValues, QueryTree.QueryType.INSERT);

        assertThrows(Exception.class, () -> {
            LogicalPlanNode n = planner.plan(insertQuery);
            PhysicalPlanNode p = optimizer.optimize(n);
            Executor e = executorFactory.createExecutor(p);
            executionEngine.execute(e);
        });
    }
}
