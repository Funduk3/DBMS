package ru.open.cu.student.index.btree;

import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.TID;

import java.util.List;

/**
 * Интерфейс B+-Tree индекса.
 * <p>
 * Поддерживает:
 * - O(log n) поиск по равенству
 * - O(log n + k) range-запросы
 * - Упорядоченный обход (для сортировки)
 * - Гарантированная сбалансированность
 * <p>
 * Отличие от обычного B-Tree:
 * - Все данные хранятся в листьях
 * - Листья связаны двусвязным списком
 * - Внутренние узлы содержат только навигационные ключи
 */
public interface BPlusTreeIndex extends Index {
    /**
     * Поиск точного совпадения.
     *
     * @return список TID'ов для ключа (может быть несколько при дубликатах)
     */
    List<TID> search(Comparable<?> key);

    /**
     * Range search: все ключи в диапазоне [from, to].
     *
     * @param inclusive если true — оба конца включены
     * @return упорядоченный список TID'ов
     */
    List<TID> rangeSearch(Comparable<?> from, Comparable<?> to, boolean inclusive);

    /**
     * Все ключи > value (или >= в зависимости от inclusive).
     */
    List<TID> searchGreaterThan(Comparable<?> value, boolean inclusive);

    /**
     * Все ключи < value (или <= в зависимости от inclusive).
     */
    List<TID> searchLessThan(Comparable<?> value, boolean inclusive);

    /**
     * Полное сканирование в упорядоченном порядке.
     */
    List<TID> scanAll();

    /**
     * Высота дерева (1 для одного листа, 2 для корня + листья, и т.д.).
     */
    int getHeight();

    /**
     * Порядок дерева (M, branching factor).
     */
    int getOrder();
}
