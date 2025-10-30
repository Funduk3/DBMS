package ru.open.cu.student.optimizer;

import ru.open.cu.student.optimizer.node.*;
import ru.open.cu.student.planner.node.*;

public class OptimizerImpl implements Optimizer {

    @Override
    public PhysicalPlanNode optimize(LogicalPlanNode logicalPlan) {
        if (logicalPlan == null) {
            throw new IllegalArgumentException("Logical plan is null");
        }

        if (logicalPlan instanceof CreateTableNode ln) {
            return new PhysicalCreateNode(ln.getTableDefinition());

        } else if (logicalPlan instanceof InsertNode ln) {
            return new PhysicalInsertNode(ln.getTableDefinition(), ln.getValues());

        } else if (logicalPlan instanceof ScanNode ln) {
            return new PhysicalSeqScanNode(ln.getTableDefinition());
        } else if (logicalPlan instanceof FilterNode ln) {
            PhysicalPlanNode physicalChild = optimize(ln.getChild());
            return new PhysicalFilterNode(physicalChild, ln.getPredicate());
        } else if (logicalPlan instanceof ProjectNode ln) {
            PhysicalPlanNode physicalChild = optimize(ln.getChild());
            return new PhysicalProjectNode(physicalChild, ln.getTargetList());
        }

        throw new UnsupportedOperationException(
                "Unsupported logical node type: " + logicalPlan.getClass().getSimpleName()
        );
    }
}