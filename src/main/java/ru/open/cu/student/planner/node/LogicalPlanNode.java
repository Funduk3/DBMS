package ru.open.cu.student.planner.node;


import java.util.List;

public abstract class LogicalPlanNode {

    protected final String nodeType;
    protected List<String> outputColumns;

    protected LogicalPlanNode(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getNodeType() {
        return nodeType;
    }

    public List<String> getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(List<String> outputColumns) {
        this.outputColumns = outputColumns;
    }

    public String prettyPrint(String indent) {
        return indent + nodeType + "\n";
    }

    @Override
    public String toString() {
        return nodeType;
    }
}
