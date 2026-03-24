package ru.open.cu.student.execution;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.operation.OperationManager;
import ru.open.cu.student.execution.executors.*;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.index.btree.BPlusTreeIndex;
import ru.open.cu.student.index.hash.HashIndex;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.optimizer.node.*;

public class ExecutorFactoryImpl implements ExecutorFactory {

    private final CatalogManager catalogManager;
    private final OperationManager operationManager;
    private final PageFileManager pageFileManager;

    public ExecutorFactoryImpl(CatalogManager catalogManager, OperationManager operationManager) {
        this.catalogManager = catalogManager;
        this.operationManager = operationManager;
        this.pageFileManager = null; // Для обратной совместимости
    }

    public ExecutorFactoryImpl(CatalogManager catalogManager, OperationManager operationManager, PageFileManager pageFileManager) {
        this.catalogManager = catalogManager;
        this.operationManager = operationManager;
        this.pageFileManager = pageFileManager;
    }

    @Override
    public Executor createExecutor(PhysicalPlanNode plan) {
        if (plan == null) {
            throw new IllegalArgumentException("Physical plan is null");
        }

        if (plan instanceof PhysicalCreateNode create) {
            return new CreateTableExecutor(catalogManager, create.getTableDefinition());

        } else if (plan instanceof PhysicalCreateIndexNode createIndex) {
            if (pageFileManager == null) {
                throw new IllegalStateException("PageFileManager is required for CREATE INDEX");
            }
            if (!(catalogManager instanceof CatalogManagerImpl)) {
                throw new IllegalStateException("CatalogManagerImpl is required for CREATE INDEX");
            }
            return new CreateIndexExecutor(
                (CatalogManagerImpl) catalogManager,
                pageFileManager,
                operationManager,
                createIndex.getIndexName(),
                createIndex.getTableName(),
                createIndex.getColumnName(),
                createIndex.getIndexType()
            );

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
        } else if (plan instanceof PhysicalIndexScanNode) {
            PhysicalIndexScanNode indexNode = (PhysicalIndexScanNode) plan;

            if (indexNode.getIndexType() == IndexType.BTREE) {
                BPlusTreeIndex index = (BPlusTreeIndex) catalogManager.getIndex(
                        indexNode.getIndexName()
                );
                TableDefinition table = catalogManager.getTable(indexNode.getTableName());

                if (indexNode.hasRange()) {
                    return new BTreeIndexScanExecutor(operationManager, index, indexNode.getRangeFrom(), indexNode.getRangeTo(), table);
                } else {
                    return new BTreeIndexScanExecutor(operationManager, index, indexNode.getValue(), table);
                }
            } else if (indexNode.getIndexType() == IndexType.HASH) {
                HashIndex index = (HashIndex) catalogManager.getIndex(
                        indexNode.getIndexName()
                );
                TableDefinition table = catalogManager.getTable(
                        indexNode.getTableName()
                );
                Comparable searchKey = indexNode.getValue();

                return new HashIndexScanExecutor(operationManager, index, searchKey, table);
            }
        }

        throw new UnsupportedOperationException(
                "Unsupported physical plan node: " + plan.getClass().getSimpleName()
        );
    }

    private ru.open.cu.student.catalog.model.TableDefinition extractTableDefinition(PhysicalPlanNode node) {
        if (node instanceof PhysicalSeqScanNode scan) {
            return scan.getTableDefinition();
        } else if (node instanceof PhysicalIndexScanNode indexScan) {
            return catalogManager.getTable(indexScan.getTableName());
        } else if (node instanceof PhysicalFilterNode filter) {
            return extractTableDefinition(filter.getChild());
        } else if (node instanceof PhysicalProjectNode project) {
            return extractTableDefinition(project.getChild());
        }

        throw new IllegalStateException("Cannot extract TableDefinition from node: " + node.getClass().getSimpleName());
    }
}