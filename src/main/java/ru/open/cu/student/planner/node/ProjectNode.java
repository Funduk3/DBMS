package ru.open.cu.student.planner.node;

import ru.open.cu.student.parser.nodes.TargetEntry;

import java.util.List;
import java.util.stream.Collectors;

public class ProjectNode extends LogicalPlanNode {

    private final LogicalPlanNode child;
    private final List<TargetEntry> targetList;

    public ProjectNode(LogicalPlanNode child, List<TargetEntry> targetList) {
        super("Project");
        this.child = child;
        this.targetList = targetList;
        this.outputColumns = targetList.stream()
                .map(te -> te.alias != null ? te.alias : te.expr.toString())
                .collect(Collectors.toList());
    }

    public LogicalPlanNode getChild() {
        return child;
    }

    public List<TargetEntry> getTargetList() {
        return targetList;
    }

    @Override
    public String prettyPrint(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append("Project(");
        sb.append(targetList.stream()
                .map(te -> te.alias != null ? te.alias : te.expr.toString())
                .collect(Collectors.joining(", ")));
        sb.append(")\n");
        sb.append(child.prettyPrint(indent + "  "));
        return sb.toString();
    }
}