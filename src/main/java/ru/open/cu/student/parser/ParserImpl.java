package ru.open.cu.student.parser;

import ru.open.cu.student.lexer.Token;
import ru.open.cu.student.parser.nodes.*;
import ru.open.cu.student.parser.nodes.Statements.CreateStmt;
import ru.open.cu.student.parser.nodes.Statements.CreateIndexStmt;
import ru.open.cu.student.parser.nodes.Statements.InsertStmt;
import ru.open.cu.student.parser.nodes.Statements.SelectStmt;

import java.util.ArrayList;
import java.util.List;

public class ParserImpl implements Parser {
    private int offset = 0;

    @Override
    public AstNode parse(List<Token> tokens) {
        offset = 0;
        Token queryType = tokens.get(offset);
        offset++;
        return switch (queryType.getType()) {
            case "SELECT" -> parseSelectStmt(tokens);
            case "INSERT" -> parseInsertStmt(tokens);
            case "CREATE" -> parseCreateStmt(tokens);
            default -> throw new IllegalArgumentException("Incorrect sql query");
        };
    }

    private AstNode parseSelectStmt(List<Token> tokens) {
        List<ResTarget> resTargetList = new ArrayList<>();
        while (offset < tokens.size() && !tokens.get(offset).getType().equals("FROM")) {
            if (tokens.get(offset).getType().equals("COMMA")) {
                offset++;
                continue;
            }

            if (tokens.get(offset).getType().equals("STAR")) {
                resTargetList.add(new ResTarget(new ColumnRef("*"), null));
                offset++;
                continue;
            }

            Expr expr = parsePrimary(tokens);
            String alias = null;

            if (offset < tokens.size() && tokens.get(offset).getType().equals("AS")) {
                offset++;
                if (offset < tokens.size() && tokens.get(offset).getType().equals("IDENT")) {
                    alias = tokens.get(offset).getValue();
                    offset++;
                } else {
                    throw new IllegalArgumentException("Expected alias name after AS");
                }
            } else if (offset < tokens.size() && tokens.get(offset).getType().equals("IDENT")) {
                alias = tokens.get(offset).getValue();
                offset++;
            }

            resTargetList.add(new ResTarget(expr, alias));
        }

        if (resTargetList.isEmpty()) throw new IllegalArgumentException("Incorrect sql query");

        offset++;
        List<RangeVar> rangeVarList = new ArrayList<>();
        while (offset < tokens.size() && !tokens.get(offset).getType().equals("WHERE") && !tokens.get(offset).getType().equals("SEMICOLON")) {
            if (tokens.get(offset).getType().equals("COMMA")) {
                offset++;
                continue;
            }

            String tableName = tokens.get(offset).getValue();
            offset++;

            String alias = null;
            if (offset < tokens.size() && tokens.get(offset).getType().equals("AS")) {
                offset++;
                if (offset < tokens.size() && tokens.get(offset).getType().equals("IDENT")) {
                    alias = tokens.get(offset).getValue();
                    offset++;
                } else {
                    throw new IllegalArgumentException("Expected alias name after AS");
                }
            } else if (offset < tokens.size() && tokens.get(offset).getType().equals("IDENT")) {
                alias = tokens.get(offset).getValue();
                offset++;
            }

            rangeVarList.add(new RangeVar(null, tableName, alias));
        }

        if (rangeVarList.isEmpty()) throw new IllegalArgumentException("Incorrect sql query");

        if (offset == tokens.size() || tokens.get(offset).getType().equals("SEMICOLON")) {
            return new SelectStmt(resTargetList, rangeVarList, null);
        }

        offset++;
        AExpr whereClause = parseWhereClause(tokens);
        return new SelectStmt(resTargetList, rangeVarList, whereClause);
    }

    AExpr parseWhereClause(List<Token> tokens) {
        Expr left = parsePrimary(tokens);
        String tt = tokens.get(offset).getType();
        if (isOperatorType(tt)) {
            String op = tokens.get(offset++).getType();
            Expr right = parsePrimary(tokens);
            return new AExpr(op, left, right);
        } else {
            throw new IllegalArgumentException("Expected operator in WHERE clause, found: " + tt);
        }
    }

    private boolean isOperatorType(String tt) {
        return "EQ".equals(tt) || "GT".equals(tt) || "LT".equals(tt)
                || "GE".equals(tt) || "LE".equals(tt) || "NEQ".equals(tt);
    }

    Expr parsePrimary(List<Token> tokens) {
        if (offset >= tokens.size())
            throw new IllegalArgumentException("Unexpected end of input while parsing expression");

        Token tk = tokens.get(offset++);
        switch (tk.getType()) {
            case "IDENT":
                if (offset < tokens.size() && tokens.get(offset).getType().equals("DOT")) {
                    offset++;
                    if (offset >= tokens.size())
                        throw new IllegalArgumentException("Expected column name after dot");
                    Token columnToken = tokens.get(offset++);
                    if (!columnToken.getType().equals("IDENT")) {
                        throw new IllegalArgumentException("Expected column name after dot, got: " + columnToken.getType());
                    }
                    return new ColumnRef(tk.getValue() + "." + columnToken.getValue());
                }
                return new ColumnRef(tk.getValue());
            case "NUMBER":
                try {
                    String value = tk.getValue();
                    if (value.contains(".")) {
                        double d = Double.parseDouble(value);
                        return new AConst(d);
                    } else {
                        int v = Integer.parseInt(value);
                        return new AConst(v);
                    }
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid number literal: " + tk.getValue());
                }
            case "STRING":
                return new AConst(tk.getValue());
            case "STAR":
                throw new IllegalArgumentException("Unexpected STAR token in expression");
            default:
                throw new IllegalArgumentException("Unexpected token in expression: " + tk);
        }
    }

    private AstNode parseInsertStmt(List<Token> tokens) {
        List<ResTarget> intoClause = new ArrayList<>();
        List<ResTarget> valuesClause = new ArrayList<>();
        RangeVar tableName = null;

        // Parse INTO keyword
        if (offset < tokens.size() && tokens.get(offset).getType().equals("INTO")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected INTO keyword in INSERT statement");
        }

        // Parse table name
        if (offset < tokens.size() && tokens.get(offset).getType().equals("IDENT")) {
            tableName = new RangeVar(null, tokens.get(offset).getValue(), null);
            offset++;
        } else {
            throw new IllegalArgumentException("Expected table name in INSERT statement");
        }

        // Parse optional column list: (col1, col2, ...)
        if (offset < tokens.size() && tokens.get(offset).getType().equals("LPAREN")) {
            offset++;

            while (offset < tokens.size() && !tokens.get(offset).getType().equals("RPAREN")) {
                if (tokens.get(offset).getType().equals("COMMA")) {
                    offset++;
                    continue;
                }
                if (tokens.get(offset).getType().equals("IDENT")) {
                    intoClause.add(new ResTarget(new ColumnRef(tokens.get(offset).getValue()), null));
                    offset++;
                }
            }

            if (offset < tokens.size() && tokens.get(offset).getType().equals("RPAREN")) {
                offset++;
            } else {
                throw new IllegalArgumentException("Expected ')' after column list");
            }
        }

        // Parse VALUES keyword
        if (offset < tokens.size() && tokens.get(offset).getType().equals("VALUES")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected VALUES keyword");
        }

        // Parse values list: (val1, val2, ...)
        if (offset < tokens.size() && tokens.get(offset).getType().equals("LPAREN")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected '(' after VALUES");
        }

        while (offset < tokens.size() && !tokens.get(offset).getType().equals("RPAREN")) {
            if (tokens.get(offset).getType().equals("COMMA")) {
                offset++;
                continue;
            }

            // Parse value (can be STRING, NUMBER, IDENT, etc.)
            Token valueToken = tokens.get(offset);
            if (valueToken.getType().equals("STRING") ||
                    valueToken.getType().equals("VARCHAR") ||
                    valueToken.getType().equals("NUMBER") ||
                    valueToken.getType().equals("IDENT")) {
                valuesClause.add(new ResTarget(new ColumnRef(valueToken.getValue()), null));
                offset++;
            } else {
                throw new IllegalArgumentException("Expected value in VALUES list, got: " + valueToken.getType());
            }
        }

        if (offset < tokens.size() && tokens.get(offset).getType().equals("RPAREN")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected ')' after VALUES list");
        }

        return new InsertStmt(tableName, intoClause, valuesClause);
    }

    private AstNode parseCreateStmt(List<Token> tokens) {
        if (offset >= tokens.size()) {
            throw new IllegalArgumentException("Unexpected end of input after CREATE");
        }
        
        String createType = tokens.get(offset).getType();
        if ("TABLE".equals(createType)) {
            return parseCreateTableStmt(tokens);
        } else if ("INDEX".equals(createType)) {
            return parseCreateIndexStmt(tokens);
        } else {
            throw new IllegalArgumentException("Expected TABLE or INDEX after CREATE, got: " + createType);
        }
    }

    private AstNode parseCreateTableStmt(List<Token> tokens) {
        RangeVar tableName = null;
        List<ColumnDef> columns = new ArrayList<>();
        if (offset < tokens.size() && tokens.get(offset).getType().equals("TABLE")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected TABLE keyword in sql statement");
        }

        tableName = new RangeVar(null, tokens.get(offset).getValue(), null);
        offset++;

        if (offset < tokens.size() && tokens.get(offset).getType().equals("LPAREN")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected '(' after table name");
        }

        while (offset < tokens.size() && !tokens.get(offset).getType().equals("RPAREN")) {
            if (tokens.get(offset).getType().equals("COMMA")) {
                offset++;
                continue;
            }

            String colName = tokens.get(offset).getValue();
            offset++;

            if (offset >= tokens.size())
                throw new IllegalArgumentException("Unexpected end of input while parsing column definition");

            String typeName = tokens.get(offset).getValue();
            offset++;

            columns.add(new ColumnDef(colName, new TypeName(typeName)));
        }

        if (offset < tokens.size() && tokens.get(offset).getType().equals("RPAREN")) {
            offset++;
        } else {
            throw new IllegalArgumentException("Expected ')' after column definitions");
        }

        return new CreateStmt(tableName, columns);
    }

    private AstNode parseCreateIndexStmt(List<Token> tokens) {
        // CREATE INDEX index_name ON table_name(column_name) [USING HASH|BTREE]
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("INDEX")) {
            throw new IllegalArgumentException("Expected INDEX keyword after CREATE");
        }
        offset++; // пропускаем INDEX
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("IDENT")) {
            throw new IllegalArgumentException("Expected index name after INDEX");
        }
        String indexName = tokens.get(offset).getValue();
        offset++;
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("ON")) {
            throw new IllegalArgumentException("Expected ON keyword after index name");
        }
        offset++; // пропускаем ON
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("IDENT")) {
            throw new IllegalArgumentException("Expected table name after ON");
        }
        String tableName = tokens.get(offset).getValue();
        offset++;
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("LPAREN")) {
            throw new IllegalArgumentException("Expected '(' after table name");
        }
        offset++; // пропускаем (
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("IDENT")) {
            throw new IllegalArgumentException("Expected column name after '('");
        }
        String columnName = tokens.get(offset).getValue();
        offset++;
        
        if (offset >= tokens.size() || !tokens.get(offset).getType().equals("RPAREN")) {
            throw new IllegalArgumentException("Expected ')' after column name");
        }
        offset++; // пропускаем )
        
        // Опциональный USING HASH или USING BTREE
        String indexType = "BTREE"; // по умолчанию
        if (offset < tokens.size() && tokens.get(offset).getType().equals("USING")) {
            offset++; // пропускаем USING
            if (offset >= tokens.size()) {
                throw new IllegalArgumentException("Expected index type (HASH or BTREE) after USING");
            }
            String type = tokens.get(offset).getType();
            if ("HASH".equals(type)) {
                indexType = "HASH";
            } else if ("BTREE".equals(type)) {
                indexType = "BTREE";
            } else {
                throw new IllegalArgumentException("Expected HASH or BTREE after USING, got: " + type);
            }
            offset++;
        }
        
        return new CreateIndexStmt(indexName, tableName, columnName, indexType);
    }
}