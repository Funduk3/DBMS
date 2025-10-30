package ru.open.cu.student.planner.node;

import ru.open.cu.student.ast.Expr;

public class FilterNode extends LogicalPlanNode {

    private final LogicalPlanNode child;
    private final Expr predicate;

    public FilterNode(LogicalPlanNode child, Expr predicate) {
        super("Filter");
        this.child = child;
        this.predicate = predicate;
        this.outputColumns = child.getOutputColumns();
    }

    public LogicalPlanNode getChild() {
        return child;
    }

    public Expr getPredicate() {
        return predicate;
    }

    @Override
    public String prettyPrint(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("Filter(").append(predicate).append(")\n");
        sb.append(child.prettyPrint(indent + "  "));
        return sb.toString();
    }
}