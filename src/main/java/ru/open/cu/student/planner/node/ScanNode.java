package ru.open.cu.student.planner.node;

import ru.open.cu.student.catalog.model.TableDefinition;

import java.util.stream.Collectors;

public class ScanNode extends LogicalPlanNode {

    private final TableDefinition tableDefinition;

    public ScanNode(TableDefinition tableDefinition) {
        super("Scan");
        this.tableDefinition = tableDefinition;
        this.outputColumns = tableDefinition.getColumns().stream()
                .map(col -> tableDefinition.getName() + "." + col.getName())
                .collect(Collectors.toList());
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public String prettyPrint(String indent) {
        return indent + "Scan(" + tableDefinition.getName() + ")\n";
    }
}