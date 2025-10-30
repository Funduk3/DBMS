package ru.open.cu.student.execution;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.execution.executors.*;
import ru.open.cu.student.optimizer.node.*;

public class ExecutorFactoryImpl implements ExecutorFactory {

    private final CatalogManager catalogManager;
    private final OperationManager operationManager;

    public ExecutorFactoryImpl(CatalogManager catalogManager, OperationManager operationManager) {
        this.catalogManager = catalogManager;
        this.operationManager = operationManager;
    }

    @Override
    public Executor createExecutor(PhysicalPlanNode plan) {
        if (plan == null) {
            throw new IllegalArgumentException("Physical plan is null");
        }

        if (plan instanceof PhysicalCreateNode create) {
            return new CreateTableExecutor(catalogManager, create.getTableDefinition());

        } else if (plan instanceof PhysicalInsertNode insert) {
            return new InsertExecutor(
                    operationManager,
                    insert.getTableDefinition(),
                    insert.getValues()
            );

        } else if (plan instanceof PhysicalSeqScanNode scan) {
            return new SeqScanExecutor(
                    operationManager,
                    scan.getTableDefinition()
            );

        } else if (plan instanceof PhysicalFilterNode filter) {
            Executor childExecutor = createExecutor(filter.getChild());

            return new FilterExecutor(
                    childExecutor,
                    filter.getPredicate(),
                    extractTableDefinition(filter.getChild())
            );

        } else if (plan instanceof PhysicalProjectNode project) {
            Executor childExecutor = createExecutor(project.getChild());

            return new ProjectExecutor(
                    childExecutor,
                    project.getTargetList(),
                    extractTableDefinition(project.getChild())
            );
        }

        throw new UnsupportedOperationException(
                "Unsupported physical plan node: " + plan.getClass().getSimpleName()
        );
    }

    private ru.open.cu.student.catalog.model.TableDefinition extractTableDefinition(PhysicalPlanNode node) {
        if (node instanceof PhysicalSeqScanNode scan) {
            return scan.getTableDefinition();
        } else if (node instanceof PhysicalFilterNode filter) {
            return extractTableDefinition(filter.getChild());
        } else if (node instanceof PhysicalProjectNode project) {
            return extractTableDefinition(project.getChild());
        }

        throw new IllegalStateException("Cannot extract TableDefinition from node: " + node.getClass().getSimpleName());
    }
}