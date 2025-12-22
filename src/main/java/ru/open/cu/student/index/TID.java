package ru.open.cu.student.index;

/**
 * Tuple IDentifier - физический адрес строки в таблице.
 * В PostgreSQL это называется ctid (current tuple id).
 * <p>
 * Формат: (pageId, slotId)
 *
 * @param pageId номер страницы (0-indexed)
 * @param slotId номер слота на странице (0-indexed)
 */
public record TID(int pageId, short slotId) {
    /**
     * Конструктор с автоматическим приведением slotId к short.
     */
    public TID(int pageId, int slotId) {
        this(pageId, (short) slotId);
    }
}
