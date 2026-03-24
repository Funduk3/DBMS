package ru.open.cu.student.optimizer;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.optimizer.node.*;
import ru.open.cu.student.parser.nodes.AConst;
import ru.open.cu.student.parser.nodes.AExpr;
import ru.open.cu.student.parser.nodes.ColumnRef;
import ru.open.cu.student.parser.nodes.Expr;
import ru.open.cu.student.planner.node.*;

public class OptimizerImpl implements Optimizer {

    private final CatalogManager catalogManager;

    public OptimizerImpl() {
        this.catalogManager = null;
    }

    public OptimizerImpl(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    @Override
    public PhysicalPlanNode optimize(LogicalPlanNode logicalPlan) {
        if (logicalPlan == null) {
            throw new IllegalArgumentException("Logical plan is null");
        }

        if (logicalPlan instanceof CreateTableNode ln) {
            return new PhysicalCreateNode(ln.getTableDefinition());

        } else if (logicalPlan instanceof CreateIndexNode ln) {
            return new PhysicalCreateIndexNode(
                ln.getIndexName(),
                ln.getTableName(),
                ln.getColumnName(),
                ln.getIndexType()
            );

        } else if (logicalPlan instanceof InsertNode ln) {
            return new PhysicalInsertNode(ln.getTableDefinition(), ln.getValues());

        } else if (logicalPlan instanceof ScanNode ln) {
            return new PhysicalSeqScanNode(ln.getTableDefinition());
        } else if (logicalPlan instanceof FilterNode ln) {
            // Проверяем, можно ли использовать индекс
            PhysicalPlanNode physicalChild = optimize(ln.getChild());
            
            // Если child - это SeqScan, проверяем, можно ли заменить на IndexScan
            if (physicalChild instanceof PhysicalSeqScanNode seqScan && catalogManager != null) {
                PhysicalIndexScanNode indexScan = tryUseIndex(seqScan, ln.getPredicate());
                if (indexScan != null) {
                    // Используем IndexScan вместо SeqScan
                    // FilterNode оставляем, так как он может быть нужен для дополнительной фильтрации
                    return new PhysicalFilterNode(indexScan, ln.getPredicate());
                }
            }
            
            return new PhysicalFilterNode(physicalChild, ln.getPredicate());
        } else if (logicalPlan instanceof ProjectNode ln) {
            PhysicalPlanNode physicalChild = optimize(ln.getChild());
            return new PhysicalProjectNode(physicalChild, ln.getTargetList());
        }

        throw new UnsupportedOperationException(
                "Unsupported logical node type: " + logicalPlan.getClass().getSimpleName()
        );
    }

    /**
     * Пытается найти подходящий индекс для замены SeqScan на IndexScan
     * @param seqScan узел SeqScan
     * @param predicate предикат фильтрации
     * @return PhysicalIndexScanNode или null, если индекс не найден
     */
    private PhysicalIndexScanNode tryUseIndex(PhysicalSeqScanNode seqScan, Expr predicate) {
        if (!(predicate instanceof AExpr aExpr)) {
            return null;
        }

        // Извлекаем информацию о колонке и значении из предиката
        String columnName = null;
        Comparable<?> value = null;
        String operator = aExpr.getOp();

        // Проверяем, что оператор поддерживается для индекса
        // Для Hash индекса - только равенство (=)
        // Для BTree индекса - все операторы сравнения
        if (!isIndexableOperator(operator)) {
            return null;
        }

        // Извлекаем колонку и значение
        Expr left = aExpr.getLeft();
        Expr right = aExpr.getRight();

        if (left instanceof ColumnRef && right instanceof AConst) {
            ColumnRef colRef = (ColumnRef) left;
            AConst constVal = (AConst) right;
            columnName = extractColumnName(colRef);
            value = (Comparable<?>) constVal.getVal();
        } else if (left instanceof AConst && right instanceof ColumnRef) {
            // Обратный порядок: value = column
            AConst constVal = (AConst) left;
            ColumnRef colRef = (ColumnRef) right;
            columnName = extractColumnName(colRef);
            value = (Comparable<?>) constVal.getVal();
            // Для обратного порядка нужно инвертировать оператор
            operator = invertOperator(operator);
        } else {
            return null;
        }

        if (columnName == null || value == null) {
            return null;
        }

        // Ищем индекс для этой колонки
        Index index = (Index) catalogManager.findIndexByTableAndColumn(
                seqScan.getTableDefinition().getName(),
                columnName
        );

        if (index == null) {
            return null;
        }

        // Создаем PhysicalIndexScanNode
        PhysicalIndexScanNode indexScan = new PhysicalIndexScanNode(
                index.getName(),
                seqScan.getTableDefinition().getName(),
                index.getType()
        );

        // Настраиваем индекс в зависимости от оператора
        if (index.getType() == IndexType.HASH) {
            // Hash индекс поддерживает только равенство
            if ("EQ".equals(operator)) {
                indexScan.setValue(value);
                return indexScan;
            }
        } else if (index.getType() == IndexType.BTREE) {
            // BTree индекс поддерживает range scans
            if ("EQ".equals(operator)) {
                indexScan.setValue(value);
                return indexScan;
            } else if ("GT".equals(operator)) {
                indexScan.setRange(value, null, false);
                return indexScan;
            } else if ("GE".equals(operator)) {
                indexScan.setRange(value, null, true);
                return indexScan;
            } else if ("LT".equals(operator)) {
                indexScan.setRange(null, value, false);
                return indexScan;
            } else if ("LE".equals(operator)) {
                indexScan.setRange(null, value, true);
                return indexScan;
            }
        }

        return null;
    }

    private String extractColumnName(ColumnRef colRef) {
        String col = colRef.getColumn();
        // Если колонка указана как "table.column", извлекаем только имя колонки
        if (col.contains(".")) {
            return col.substring(col.lastIndexOf('.') + 1);
        }
        return col;
    }

    private boolean isIndexableOperator(String operator) {
        return "EQ".equals(operator) ||
               "GT".equals(operator) ||
               "GE".equals(operator) ||
               "LT".equals(operator) ||
               "LE".equals(operator);
    }

    private String invertOperator(String operator) {
        // Инвертируем оператор для обратного порядка операндов
        return switch (operator) {
            case "GT" -> "LT";
            case "GE" -> "LE";
            case "LT" -> "GT";
            case "LE" -> "GE";
            default -> operator; // EQ остается без изменений
        };
    }
}