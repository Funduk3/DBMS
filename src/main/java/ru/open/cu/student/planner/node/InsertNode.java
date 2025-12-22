package ru.open.cu.student.planner.node;

import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.parser.nodes.Expr;

import java.util.List;

public class InsertNode extends LogicalPlanNode {

    private final TableDefinition tableDefinition;
    private final List<Expr> values;

    public InsertNode(TableDefinition tableDefinition, List<Expr> values) {
        super("Insert");
        this.tableDefinition = tableDefinition;
        this.values = values;
        this.outputColumns = List.of();
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    public List<Expr> getValues() {
        return values;
    }

    @Override
    public String prettyPrint(String indent) {
        return indent + "Insert(" + tableDefinition.getName() + ", values=" + values + ")\n";
    }
}