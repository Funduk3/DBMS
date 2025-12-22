package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.parser.nodes.Expr;

public class PhysicalFilterNode extends PhysicalPlanNode {

    private final PhysicalPlanNode child;
    private final Expr predicate;

    public PhysicalFilterNode(PhysicalPlanNode child, Expr predicate) {
        super("PhysicalFilter");
        this.child = child;
        this.predicate = predicate;
    }

    public PhysicalPlanNode getChild() {
        return child;
    }

    public Expr getPredicate() {
        return predicate;
    }

    @Override
    public String prettyPrint(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("PhysicalFilter(").append(predicate).append(")\n");
        sb.append(child.prettyPrint(indent + "  "));
        return sb.toString();
    }
}