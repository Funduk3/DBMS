package ru.open.cu.student.semantic;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.parser.nodes.*;
import ru.open.cu.student.parser.nodes.Statements.CreateStmt;
import ru.open.cu.student.parser.nodes.Statements.InsertStmt;
import ru.open.cu.student.parser.nodes.Statements.SelectStmt;

import java.util.*;

public class SemanticAnalyzerImpl implements SemanticAnalyzer {
    CatalogManager catalogManager;

    public SemanticAnalyzerImpl(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    @Override
    public QueryTree analyze(AstNode ast, CatalogManager catalog) {
        if (ast instanceof SelectStmt) {
            return analyzeSelect((SelectStmt) ast, catalog);
        } else if (ast instanceof InsertStmt) {
            return analyzeInsert((InsertStmt) ast, catalog);
        } else if (ast instanceof CreateStmt) {
            return analyzeCreate((CreateStmt) ast, catalog);
        } else {
            throw new IllegalArgumentException("Unknown statement type: " + ast.getClass().getName());
        }
    }

    private QueryTree analyzeSelect(SelectStmt stmt, CatalogManager catalog) {
        List<TableDefinition> tables = new ArrayList<>();
        Map<String, TableDefinition> tableMap = new HashMap<>();
        Map<String, String> aliasMap = new HashMap<>();

        for (RangeVar rv : stmt.getFromClause()) {
            String tableName = rv.getRelname();
            TableDefinition table = catalog.getTable(tableName);
            if (table == null) {
                throw new SemanticException("Table not found: " + tableName);
            }
            tables.add(table);
            tableMap.put(tableName, table);

            if (rv.getAlias() != null) {
                aliasMap.put(rv.getAlias(), tableName);
                tableMap.put(rv.getAlias(), table);
            }
        }

        List<ColumnDefinition> targetColumns = new ArrayList<>();
        for (ResTarget target : stmt.getTargetList()) {
            Expr expr = target.getVal();
            if (expr instanceof ColumnRef colRef) {
                String colName = colRef.getColumn();

                if ("*".equals(colName)) {
                    for (TableDefinition table : tables) {
                        targetColumns.addAll(table.getColumns());
                    }
                    continue;
                }

                ColumnDefinition column = resolveColumn(colName, tables, tableMap, aliasMap, catalog);

                if (target.getName() != null) {
                    targetColumns.add(column);
                } else {
                    targetColumns.add(column);
                }
            } else {
                throw new SemanticException("Unsupported expression type in SELECT: " + expr.getClass().getSimpleName());
            }
        }

        AExpr whereClause = stmt.getWhereClause();
        if (whereClause != null) {
            validateWhereClause(whereClause, tables, tableMap, aliasMap, catalog);
        }

        return new QueryTree(targetColumns, tables, whereClause);
    }

    private QueryTree analyzeInsert(InsertStmt stmt, CatalogManager catalog) {
        String tableName = stmt.getTableName().getRelname();
        TableDefinition table = catalog.getTable(tableName);
        if (table == null) {
            throw new SemanticException("Table not found: " + tableName);
        }

        List<ResTarget> intoCols = stmt.getIntoClause();
        List<ResTarget> values = stmt.getValuesClause();

        int expectedColumnCount = intoCols.isEmpty() ? table.getColumns().size() : intoCols.size();

        if (values.size() != expectedColumnCount) {
            throw new SemanticException(
                    String.format("Column count mismatch. Expected %d, got %d",
                            expectedColumnCount, values.size())
            );
        }

        List<Object> typedValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            ColumnDefinition column;
            if (intoCols.isEmpty()) {
                column = table.getColumns().get(i);
            } else {
                String colName = getColumnNameFromResTarget(intoCols.get(i));
                column = catalog.getColumn(table, colName);
                if (column == null) {
                    throw new SemanticException("Column not found in INTO clause: " + colName);
                }
            }

            String valueStr = extractValueFromResTarget(values.get(i));
            Object typedValue = convertValue(valueStr, catalogManager.getType(column.getTypeOid()).getName());
            typedValues.add(typedValue);
        }

        return new QueryTree(table, typedValues, QueryTree.QueryType.INSERT);
    }

    private QueryTree analyzeCreate(CreateStmt stmt, CatalogManager catalog) {
        String tableName = stmt.getTableName().getRelname();

        if (catalog.getTable(tableName) != null) {
            throw new SemanticException("Table already exists: " + tableName);
        }

        List<ColumnDefinition> catalogColumns = new ArrayList<>();
        int position = 0;
        for (ColumnDef colDef : stmt.getColumns()) {
            String colName = colDef.colname;
            String typeName = colDef.typeName.getName();

            if (!isValidType(typeName)) {
                throw new SemanticException("Invalid column type: " + typeName);
            }

            ColumnDefinition catalogCol =
                    new ColumnDefinition(position, colName, catalogManager.getTypeByName(typeName).getOid());
            catalogColumns.add(catalogCol);
            position++;
        }

        return new QueryTree(tableName, catalogColumns, QueryTree.QueryType.CREATE);
    }

    private ColumnDefinition resolveColumn(String colName, List<TableDefinition> tables,
                                           Map<String, TableDefinition> tableMap,
                                           Map<String, String> aliasMap, CatalogManager catalog) {
        if (colName.contains(".")) {
            String[] parts = colName.split("\\.", 2);
            String tableOrAlias = parts[0];
            String actualColName = parts[1];

            String actualTableName = aliasMap.getOrDefault(tableOrAlias, tableOrAlias);
            TableDefinition table = tableMap.get(actualTableName);

            if (table == null) {
                throw new SemanticException("Table or alias not found: " + tableOrAlias);
            }

            ColumnDefinition column = catalog.getColumn(table, actualColName);
            if (column == null) {
                throw new SemanticException("Column '" + actualColName + "' not found in table: " + actualTableName);
            }
            return column;
        }

        ColumnDefinition found = null;
        String foundInTable = null;

        for (TableDefinition table : tables) {
            ColumnDefinition col = catalog.getColumn(table, colName);
            if (col != null) {
                if (found != null) {
                    throw new SemanticException("Ambiguous column name: " + colName +
                            " (found in tables: " + foundInTable + " and " + table.getName() + ")");
                }
                found = col;
                foundInTable = table.getName();
            }
        }

        if (found == null) {
            throw new SemanticException("Column not found in any table: " + colName);
        }

        return found;
    }

    private String getExpressionType(Expr expr, List<TableDefinition> tables,
                                     Map<String, TableDefinition> tableMap,
                                     Map<String, String> aliasMap, CatalogManager catalog) {
        if (expr instanceof ColumnRef) {
            String colName = ((ColumnRef) expr).getColumn();
            ColumnDefinition column = resolveColumn(colName, tables, tableMap, aliasMap, catalog);
            return column != null ? catalogManager.getType(column.getTypeOid()).getName() : "UNKNOWN";
        } else if (expr instanceof AConst) {
            Object value = ((AConst) expr).getVal();
            if (value instanceof Integer) {
                return "INT";
            } else if (value instanceof String) {
                return "STRING";
            } else if (value instanceof Double) {
                return "DOUBLE";
            }
        }
        return "UNKNOWN";
    }

    private boolean areTypesCompatible(String type1, String type2) {
        if (type1.equals(type2)) return true;

        Set<String> numericTypes = Set.of("INT", "INTEGER", "NUMBER", "DOUBLE", "FLOAT");
        if (numericTypes.contains(type1) && numericTypes.contains(type2)) {
            return true;
        }

        return false;
    }

    private void validateWhereClause(AExpr whereClause, List<TableDefinition> tables,
                                     Map<String, TableDefinition> tableMap,
                                     Map<String, String> aliasMap, CatalogManager catalog) {
        validateExpression(whereClause, tables, tableMap, aliasMap, catalog);
    }

    private void validateExpression(Expr expr, List<TableDefinition> tables,
                                    Map<String, TableDefinition> tableMap,
                                    Map<String, String> aliasMap, CatalogManager catalog) {
        if (expr instanceof ColumnRef) {
            String colName = ((ColumnRef) expr).getColumn();
            ColumnDefinition column = resolveColumn(colName, tables, tableMap, aliasMap, catalog);
            if (column == null) {
                throw new SemanticException("Column not found in WHERE clause: " + colName);
            }
        } else if (expr instanceof AConst) {
            return;
        } else if (expr instanceof AExpr aexpr) {
            validateExpression(aexpr.getLeft(), tables, tableMap, aliasMap, catalog);
            validateExpression(aexpr.getRight(), tables, tableMap, aliasMap, catalog);

            if (isComparisonOperator(aexpr.getOp())) {
                validateTypeCompatibility(aexpr.getLeft(), aexpr.getRight(),
                        tables, tableMap, aliasMap, catalog);
            }
        }
    }

    private void validateTypeCompatibility(Expr left, Expr right, List<TableDefinition> tables,
                                           Map<String, TableDefinition> tableMap,
                                           Map<String, String> aliasMap, CatalogManager catalog) {
        String leftType = getExpressionType(left, tables, tableMap, aliasMap, catalog);
        String rightType = getExpressionType(right, tables, tableMap, aliasMap, catalog);

        if (!areTypesCompatible(leftType, rightType)) {
            throw new SemanticException(
                    String.format("Type mismatch in WHERE clause: cannot compare %s with %s",
                            leftType, rightType)
            );
        }
    }

    private String getColumnNameFromResTarget(ResTarget resTarget) {
        Expr expr = resTarget.getVal();
        if (expr instanceof ColumnRef) {
            return ((ColumnRef) expr).getColumn();
        }
        throw new SemanticException("Invalid column reference in INTO clause");
    }

    private String extractValueFromResTarget(ResTarget resTarget) {
        Expr expr = resTarget.getVal();
        if (expr instanceof ColumnRef) {
            return ((ColumnRef) expr).getColumn();
        } else if (expr instanceof AConst) {
            Object value = ((AConst) expr).getVal();
            return value != null ? value.toString() : null;
        } else {
            throw new SemanticException("Unsupported value type in VALUES clause: " + expr.getClass().getSimpleName());
        }
    }

    private boolean isComparisonOperator(String op) {
        return "EQ".equals(op) || "GT".equals(op) || "LT".equals(op) ||
                "GE".equals(op) || "LE".equals(op) || "NEQ".equals(op);
    }

    private Object convertValue(String valueStr, String targetType) {
        try {
            switch (targetType.toUpperCase()) {
                case "INT":
                case "INTEGER":
                case "NUMBER":
                    String cleaned = valueStr.replace("'", "").trim();
                    return Integer.parseInt(cleaned);
                case "STRING":
                case "VARCHAR":
                case "TEXT":
                    if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                        return valueStr.substring(1, valueStr.length() - 1);
                    }
                    return valueStr;
                case "DOUBLE":
                case "FLOAT":
                    cleaned = valueStr.replace("'", "").trim();
                    return Double.parseDouble(cleaned);
                default:
                    throw new SemanticException("Unsupported type: " + targetType);
            }
        } catch (NumberFormatException e) {
            throw new SemanticException(
                    String.format("Cannot convert '%s' to type %s", valueStr, targetType)
            );
        }
    }

    private boolean isValidType(String type) {
        String upper = type.toUpperCase();
        return upper.equals("INT64") || upper.equals("INTEGER") ||
                upper.equals("STRING") || upper.equals("VARCHAR") ||
                upper.equals("NUMBER") || upper.equals("TEXT") ||
                upper.equals("DOUBLE") || upper.equals("FLOAT");
    }

    public static class SemanticException extends RuntimeException {
        public SemanticException(String message) {
            super(message);
        }
    }
}