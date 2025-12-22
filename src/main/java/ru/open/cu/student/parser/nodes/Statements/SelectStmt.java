package ru.open.cu.student.parser.nodes.Statements;

import ru.open.cu.student.parser.nodes.AExpr;
import ru.open.cu.student.parser.nodes.AstNode;
import ru.open.cu.student.parser.nodes.RangeVar;
import ru.open.cu.student.parser.nodes.ResTarget;

import java.util.List;

public class SelectStmt implements AstNode {
    public List<ResTarget> targetList;
    public List<RangeVar> fromClause;
    public AExpr whereClause;

    public SelectStmt(List<ResTarget> targets, List<RangeVar> from, AExpr where) {
        this.targetList = targets;
        this.fromClause = from;
        this.whereClause = where;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < targetList.size(); i++) {
            if (targetList.get(i) == null) {
                continue;
            }
            sb.append(targetList.get(i));
            if (i < fromClause.size() - 1) {
                sb.append(" ");
            }
        }
        sb.append("FROM ");
        for (int i = 0; i < fromClause.size(); i++) {
            if (targetList.get(i) == null) {
                continue;
            }
            sb.append(fromClause.get(i));
            if (i < fromClause.size() - 1) {
                sb.append(" ");
            }
        }
        if (whereClause != null) {
            sb.append("WHERE ");
            sb.append(whereClause.toString());
        }
        return sb.toString();
    }

    public List<ResTarget> getTargetList() {
        return targetList;
    }

    public List<RangeVar> getFromClause() {
        return fromClause;
    }

    public AExpr getWhereClause() {
        return whereClause;
    }
}
