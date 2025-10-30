package ru.open.cu.student.optimizer.node;

import ru.open.cu.student.ast.TargetEntry;

import java.util.List;

public class PhysicalProjectNode extends PhysicalPlanNode {

    private final PhysicalPlanNode child;
    private final List<TargetEntry> targetList;

    public PhysicalProjectNode(PhysicalPlanNode child, List<TargetEntry> targetList) {
        super("PhysicalProject");
        this.child = child;
        this.targetList = targetList;
    }

    public PhysicalPlanNode getChild() {
        return child;
    }

    public List<TargetEntry> getTargetList() {
        return targetList;
    }

    @Override
    public String prettyPrint(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("PhysicalProject(");
        if (targetList != null && !targetList.isEmpty()) {
            sb.append(targetList.stream()
                    .map(te -> te.alias != null ? te.alias : te.expr.toString())
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
        }
        sb.append(")\n");
        sb.append(child.prettyPrint(indent + "  "));
        return sb.toString();
    }
}