package ru.open.cu.student.semantic;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.parser.nodes.AstNode;

public interface SemanticAnalyzer {
    QueryTree analyze(AstNode ast, CatalogManager catalog);
}
