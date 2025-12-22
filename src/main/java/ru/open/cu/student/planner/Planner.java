package ru.open.cu.student.planner;

import ru.open.cu.student.semantic.QueryTree;
import ru.open.cu.student.planner.node.LogicalPlanNode;

public interface Planner {
    // Преобразование высокоуровневых элементов запроса (rangeTable, targetList, where)
    //    в соответствующие логические операторы (CreateNode, InsertNode, ScanNode, FilterNode, ProjectNode).
    // Формирование дерева логических узлов, отражающего порядок выполнения операций на логическом уровне.
    // Для SELECT: строить структуру вида (Project -> Filter -> Scan).
    // Не принимать решений о физических алгоритмах (не выбирать IndexScan vs SeqScan).
    // Вычисление и закрепление схемы выходных колонок (output schema) для каждого узла плана.
    // Обработка специальных операторов CREATE/INSERT: порождать соответствующие логические узлы CreateNode/InsertNode.
    // Подготовка логического плана к передаче в Optimizer и возврат верхнего узла дерева.
    LogicalPlanNode plan(QueryTree queryTree);
}
