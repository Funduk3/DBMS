package ru.open.cu.student.parser;

import ru.open.cu.student.lexer.Token;
import ru.open.cu.student.parser.nodes.AstNode;

import java.util.List;

public interface Parser {
    AstNode parse(List<Token> tokens);

}
