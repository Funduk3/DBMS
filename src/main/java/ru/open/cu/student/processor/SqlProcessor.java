package ru.open.cu.student.processor;

import ru.open.cu.student.semantic.QueryTree;
import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.lexer.Lexer;
import ru.open.cu.student.lexer.Token;
import ru.open.cu.student.parser.Parser;
import ru.open.cu.student.parser.nodes.AstNode;
import ru.open.cu.student.semantic.SemanticAnalyzer;

import java.util.List;

public class SqlProcessor {
    private final Lexer lexer;
    private final Parser parser;
    private final SemanticAnalyzer semanticAnalyzer;
    private final CatalogManager catalogManager;

    public SqlProcessor(Lexer lexer,
                        Parser parser,
                        SemanticAnalyzer semanticAnalyzer,
                        CatalogManager catalogManager) {
        this.lexer = lexer;
        this.parser = parser;
        this.semanticAnalyzer = semanticAnalyzer;
        this.catalogManager = catalogManager;
    }

    /**
     * Обработка SQL-запроса:
     * 1. Лексинг
     * 2. Парсинг → AST
     * 3. Семантический анализ → QueryTree
     */
    public QueryTree process(String sql) {
        // Лексинг
        List<Token> tokens = lexer.tokenize(sql);
        System.out.println("Tokens: " + tokens);

        // Парсинг
        AstNode ast = parser.parse(tokens);
        System.out.println("AST: " + ast);

        // Семантический анализ
        QueryTree queryTree = semanticAnalyzer.analyze(ast, catalogManager);
        System.out.println("QueryTree: " + queryTree);

        return queryTree;
    }

}
