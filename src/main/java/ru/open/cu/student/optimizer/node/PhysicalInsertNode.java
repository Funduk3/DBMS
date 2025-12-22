package ru.open.cu.student.optimizer.node;


import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.parser.nodes.Expr;

import java.util.List;

public class PhysicalInsertNode extends PhysicalPlanNode {

    private final TableDefinition tableDefinition;
    private final List<Expr> values;

    public PhysicalInsertNode(TableDefinition tableDefinition, List<Expr> values) {
        super("PhysicalInsert");
        this.tableDefinition = tableDefinition;
        this.values = values;
    }

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    public List<Expr> getValues() {
        return values;
    }

    @Override
    public String prettyPrint(String indent) {
        return indent + "PhysicalInsert(" + tableDefinition.getName() + ")\n";
    }
}