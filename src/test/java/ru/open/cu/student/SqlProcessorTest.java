package ru.open.cu.student;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import ru.open.cu.student.lexer.Lexer;
import ru.open.cu.student.lexer.LexerImpl;
import ru.open.cu.student.memory.manager.HeapPageFileManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.optimizer.node.PhysicalPlanNode;
import ru.open.cu.student.parser.Parser;
import ru.open.cu.student.parser.ParserImpl;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.node.LogicalPlanNode;
import ru.open.cu.student.processor.SqlProcessor;
import ru.open.cu.student.semantic.SemanticAnalyzer;
import ru.open.cu.student.semantic.SemanticAnalyzerImpl;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SqlProcessorTest {
    Lexer lexer;
    Parser parser;
    SemanticAnalyzer semanticAnalyzer;
    PageFileManager pageFileManager;
    CatalogManager catalogManager;
    SqlProcessor sqlProcessor;
    Planner planner;
    Optimizer optimizer;
    QueryExecutionEngine queryExecutionEngine;
    ExecutorFactory executorFactory;
    OperationManager operationManager;
    DefaultBufferPoolManager bufferPoolManager;
    Replacer replacer;

    @BeforeEach
    void setUp() {
        replacer = new ClockReplacer(10);
        pageFileManager = new HeapPageFileManager();
        bufferPoolManager = new DefaultBufferPoolManager(10, pageFileManager, replacer);
        lexer = new LexerImpl();
        parser = new ParserImpl();
        catalogManager = new CatalogManagerImpl(bufferPoolManager, pageFileManager);
        semanticAnalyzer = new SemanticAnalyzerImpl(catalogManager);
        operationManager = new OperationManagerImpl((CatalogManagerImpl) catalogManager, pageFileManager);
        sqlProcessor = new SqlProcessor(lexer, parser, semanticAnalyzer, catalogManager);
        planner = new PlannerImpl(catalogManager);
        optimizer = new OptimizerImpl();
        executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        queryExecutionEngine = new QueryExecutionEngineImpl();
    }

    @Test
    void createIntegrationTest() {
        String sql = "CREATE TABLE users2 (id INT64, name VARCHAR, age INT64)";
        QueryTree queryTree = sqlProcessor.process(sql);
        LogicalPlanNode logicalPlan = planner.plan(queryTree);
        PhysicalPlanNode physicalPlan = optimizer.optimize(logicalPlan);
        Executor executor = executorFactory.createExecutor(physicalPlan);
        List<Object> objects = queryExecutionEngine.execute(executor);
        objects.forEach(System.out::println);
        assertNotNull(catalogManager.getTable("users"));
        assertEquals("users", catalogManager.getTable("users").getName());
    }

    @Test
    void selectIntegrationTest() {
        String sql = "SELECT name FROM users WHERE id > 10;";

    }
}
