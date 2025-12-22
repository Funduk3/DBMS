package ru.open.cu.student.parser.nodes.Statements;

import ru.open.cu.student.parser.nodes.AstNode;
import ru.open.cu.student.parser.nodes.ColumnDef;
import ru.open.cu.student.parser.nodes.RangeVar;

import java.util.List;

public class CreateStmt implements AstNode {
    RangeVar tableName;
    List<ColumnDef> columns;

    public CreateStmt(RangeVar tableName, List<ColumnDef> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName.toString()).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i).colname).append(" ").append(columns.get(i).typeName.toString());
            if (i < columns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        return sb.toString();
    }

    public RangeVar getTableName() {
        return tableName;
    }

    public List<ColumnDef> getColumns() {
        return columns;
    }
}
