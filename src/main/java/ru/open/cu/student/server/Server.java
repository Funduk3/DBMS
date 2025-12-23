package ru.open.cu.student.server;

import ru.open.cu.student.lexer.LexerImpl;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.replacer.ClockReplacer;
import ru.open.cu.student.memory.replacer.Replacer;
import ru.open.cu.student.parser.ParserImpl;
import ru.open.cu.student.semantic.SemanticAnalyzerImpl;
import ru.open.cu.student.processor.SqlProcessor;
import ru.open.cu.student.planner.PlannerImpl;
import ru.open.cu.student.planner.Planner;
import ru.open.cu.student.optimizer.Optimizer;
import ru.open.cu.student.optimizer.OptimizerImpl;
import ru.open.cu.student.execution.ExecutorFactoryImpl;
import ru.open.cu.student.execution.ExecutorFactory;
import ru.open.cu.student.execution.QueryExecutionEngineImpl;
import ru.open.cu.student.execution.QueryExecutionEngine;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.catalog.operation.OperationManagerImpl;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.manager.HeapPageFileManager;

import java.net.*;
import java.io.*;
import java.util.List;

public class Server {
    public static void main(String[] args) throws IOException {
        final int PORT = 9090;
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Server listening on port " + PORT);

        Replacer replacer = new ClockReplacer(16);
        PageFileManager pageFileManager = new HeapPageFileManager();
        DefaultBufferPoolManager defaultBufferPoolManager =
                new DefaultBufferPoolManager(16, pageFileManager, replacer);
        CatalogManagerImpl catalogManager = new CatalogManagerImpl(defaultBufferPoolManager, pageFileManager);
        OperationManager operationManager = new OperationManagerImpl(catalogManager, pageFileManager);
        Planner planner = new PlannerImpl(catalogManager);
        Optimizer optimizer = new OptimizerImpl();
        ExecutorFactory executorFactory = new ExecutorFactoryImpl(catalogManager, operationManager);
        QueryExecutionEngine executionEngine = new QueryExecutionEngineImpl();

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> {
                try (
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
                ) {
                    SqlProcessor sqlProcessor = new SqlProcessor(
                            new LexerImpl(),
                            new ParserImpl(),
                            new SemanticAnalyzerImpl(catalogManager),
                            catalogManager
                    );
                    String sql;
                    while ((sql = in.readLine()) != null) {
                        if (sql.trim().equalsIgnoreCase("exit")) {
                            break;
                        }
                        try {
                            var queryTree = sqlProcessor.process(sql);
                            var logicalPlan = planner.plan(queryTree);
                            var physicalPlan = optimizer.optimize(logicalPlan);
                            var executor = executorFactory.createExecutor(physicalPlan);
                            List<Object> results = executionEngine.execute(executor);

                            if (results != null) {
                                for (Object row : results) {
                                    if (row != null) {
                                        out.println(row.toString());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            out.println("ERROR: " + e.getMessage());
                        }
                        out.println();
                    }
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
