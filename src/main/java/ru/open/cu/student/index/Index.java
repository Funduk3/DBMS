package ru.open.cu.student.index;

/**
 * Базовый интерфейс для всех типов индексов.
 * Определяет общие операции: вставка, удаление, получение метаданных.
 */
public interface Index {
    /**
     * Вставить (ключ → TID) запись в индекс.
     *
     * @param key поисковой ключ (любой Comparable тип)
     * @param tid физический адрес строки в таблице
     */
    void insert(Comparable<?> key, TID tid);

    /**
     * Получить имя индекса.
     */
    String getName();

    /**
     * Получить тип индекса.
     */
    IndexType getType();

    /**
     * Получить имя столбца, на котором построен индекс.
     */
    String getColumnName();
}
