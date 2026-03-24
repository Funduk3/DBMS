package ru.open.cu.student.lexer;

import java.util.ArrayList;
import java.util.List;

public class LexerImpl implements Lexer {

    private boolean isKeyword(String word) {
        return switch (word) {
            case "SELECT", "FROM", "WHERE",
                 "INSERT", "INTO", "VALUES",
                 "CREATE", "TABLE", "AS",
                 "INDEX", "ON", "USING", "HASH", "BTREE" -> true;
            default -> false;
        };
    }

    public List<Token> tokenize(String sql) {
        List<Token> tokens = new ArrayList<>();
        int index = 0;
        int size = sql.length();

        while (index < size) {
            char cur = sql.charAt(index);

            if (Character.isWhitespace(cur)) {
                index++;
                continue;
            }

            if (Character.isLetter(cur) || cur == '_') {
                StringBuilder sb = new StringBuilder();
                while (index < size && (Character.isLetterOrDigit(sql.charAt(index)) || sql.charAt(index) == '_')) {
                    sb.append(sql.charAt(index));
                    index++;
                }
                String word = sb.toString();
                String upper = word.toUpperCase();
                if (isKeyword(upper)) {
                    tokens.add(new Token(upper, word));
                } else {
                    tokens.add(new Token("IDENT", word));
                }
                continue;
            }

            if (Character.isDigit(cur)) {
                StringBuilder sb = new StringBuilder();
                boolean hasDecimal = false;
                while (index < size && (Character.isDigit(sql.charAt(index)) || sql.charAt(index) == '.')) {
                    if (sql.charAt(index) == '.') {
                        if (hasDecimal) {
                            throw new IllegalArgumentException("Invalid number format: multiple decimal points");
                        }
                        hasDecimal = true;
                    }
                    sb.append(sql.charAt(index));
                    index++;
                }
                tokens.add(new Token("NUMBER", sb.toString()));
                continue;
            }

            if (cur == '\'') {
                index++;
                StringBuilder sb = new StringBuilder();
                while (index < size && sql.charAt(index) != '\'') {
                    sb.append(sql.charAt(index));
                    index++;
                }
                if (index >= size || sql.charAt(index) != '\'') {
                    throw new IllegalArgumentException("Unterminated string literal");
                }
                index++;
                tokens.add(new Token("STRING", sb.toString()));
                continue;
            }

            if (cur == '.') {
                tokens.add(new Token("DOT", "."));
                index++;
                continue;
            }

            switch (cur) {
                case ',':
                    tokens.add(new Token("COMMA", ","));
                    index++;
                    break;
                case ';':
                    tokens.add(new Token("SEMICOLON", ";"));
                    index++;
                    break;
                case '(':
                    tokens.add(new Token("LPAREN", "("));
                    index++;
                    break;
                case ')':
                    tokens.add(new Token("RPAREN", ")"));
                    index++;
                    break;
                case '*':
                    tokens.add(new Token("STAR", "*"));
                    index++;
                    break;
                case '>':
                    tokens.add(new Token("GT", ">"));
                    index++;
                    break;
                case '<':
                    tokens.add(new Token("LT", "<"));
                    index++;
                    break;
                case '=':
                    tokens.add(new Token("EQ", "="));
                    index++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown symbol: " + cur);
            }
        }

        return tokens;
    }
}
