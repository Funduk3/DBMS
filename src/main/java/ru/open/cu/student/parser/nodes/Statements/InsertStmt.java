package ru.open.cu.student.parser.nodes.Statements;

import ru.open.cu.student.parser.nodes.AstNode;
import ru.open.cu.student.parser.nodes.RangeVar;
import ru.open.cu.student.parser.nodes.ResTarget;

import java.util.List;

public class InsertStmt implements AstNode {
    public RangeVar tableName;
    public List<ResTarget> intoClause;
    public List<ResTarget> valuesClause;

    public InsertStmt(RangeVar tableName, List<ResTarget> intoClause, List<ResTarget> valuesClause) {
        this.tableName = tableName;
        this.intoClause = intoClause;
        this.valuesClause = valuesClause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO");
        for (int i = 0; i < intoClause.size(); i++) {
            sb.append(intoClause.get(i));
            if (i < intoClause.size() - 1) {
                sb.append(" ");
            }
        }
        sb.append("VALUES");
        for (int i = 0; i < valuesClause.size(); i++) {
            sb.append(valuesClause.get(i));
            if (i < valuesClause.size() - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    public RangeVar getTableName() {
        return tableName;
    }

    public List<ResTarget> getIntoClause() {
        return intoClause;
    }

    public List<ResTarget> getValuesClause() {
        return valuesClause;
    }
}
