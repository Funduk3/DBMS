package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.open.cu.student.ast.ColumnDef;
import ru.open.cu.student.ast.QueryTree;
import ru.open.cu.student.ast.TypeName;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
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
import ru.open.cu.student.ast.Expr;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InsertTest {

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

    static class LiteralExpr extends Expr {
        private final Object value;

        LiteralExpr(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value == null ? "NULL" : value.toString();
        }
    }

    @Test
    void testInsertSuccess() {
        TableDefinition createTable = new TableDefinition(1, "users2", "BASE TABLE", "users_file", 0);
        List<ColumnDef> createColumns = new ArrayList<>();
        createColumns.add(new ColumnDef("id", new TypeName("INT64")));
        createColumns.add(new ColumnDef("name", new TypeName("VARCHAR")));
        QueryTree createQuery = new QueryTree(createTable, createColumns, QueryTree.QueryType.CREATE);

        LogicalPlanNode createLogical = planner.plan(createQuery);
        PhysicalPlanNode createPhysical = optimizer.optimize(createLogical);
        Executor createExecutor = executorFactory.createExecutor(createPhysical);
        executionEngine.execute(createExecutor);

        List<Object> insertValues = new ArrayList<>();
        insertValues.add(new LiteralExpr(1));
        insertValues.add(new LiteralExpr("Alice"));

        QueryTree insertQuery = new QueryTree(createTable, insertValues, QueryTree.QueryType.INSERT);

        LogicalPlanNode insertLogical = planner.plan(insertQuery);
        assertNotNull(insertLogical);
        assertEquals("Insert", insertLogical.getNodeType());

        PhysicalPlanNode insertPhysical = optimizer.optimize(insertLogical);
        assertNotNull(insertPhysical);
        assertEquals("PhysicalInsert", insertPhysical.getNodeType());

        Executor insertExecutor = executorFactory.createExecutor(insertPhysical);
        List<Object> results = executionEngine.execute(insertExecutor);

        assertNotNull(catalogManager.getTable("users2"));
        assertEquals("users2", catalogManager.getTable("users2").getName());

        assertTrue(results.isEmpty() || results.stream().allMatch(r -> r == null));
    }

    @Test
    void testInsertIntoNonexistentTable() {
        TableDefinition missingTable = new TableDefinition(99, "ghost", "BASE TABLE", "ghost_file", 0);

        List<Object> insertValues = new ArrayList<>();
        insertValues.add(new LiteralExpr(1));
        insertValues.add(new LiteralExpr("Nobody"));

        QueryTree insertQuery = new QueryTree(missingTable, insertValues, QueryTree.QueryType.INSERT);

        assertThrows(Exception.class, () -> {
            LogicalPlanNode n = planner.plan(insertQuery);
            PhysicalPlanNode p = optimizer.optimize(n);
            Executor e = executorFactory.createExecutor(p);
            executionEngine.execute(e);
        });
    }
}
