package ru.open.cu.student.lexer;

import java.util.List;

public interface Lexer {
    List<Token> tokenize(String sql);

}
