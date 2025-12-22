package ru.open.cu.student.index.hash;

import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.TID;

import java.util.List;

/**
 * Интерфейс хеш-индекса.
 * <p>
 * Поддерживает:
 * - O(1) поиск по равенству
 * - Динамическое расширение (линейное хеширование)
 * - Overflow-страницы при переполнении
 * <p>
 * НЕ поддерживает:
 * - Range-запросы
 * - Сортировку
 */
public interface HashIndex extends Index {
    /**
     * Поиск по равенству.
     *
     * @param key ключ для поиска
     * @return список TID'ов, соответствующих ключу
     * может быть несколько при коллизиях хешей
     */
    List<TID> search(Comparable<?> key);

    /**
     * Сканирование всех записей в индексе.
     * Используется для admin операций и проверки целостности.
     *
     * @return все (ключ, TID) пары в индексе в произвольном порядке
     */
    List<TID> scanAll();

    /**
     * Получить текущее количество бакетов.
     */
    int getNumBuckets();

    /**
     * Получить количество записей в индексе.
     */
    long getRecordCount();

    /**
     * Получить максимальный номер бакета (для отладки линейного хеширования).
     */
    int getMaxBucket();
}
