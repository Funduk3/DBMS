package ru.open.cu.student.index;

/**
 * Тип индекса.
 */
public enum IndexType {
    HASH,     // Hash-индекс (O(1) для равенства)
    BTREE     // B+-Tree индекс (O(log n) + range support)
}
