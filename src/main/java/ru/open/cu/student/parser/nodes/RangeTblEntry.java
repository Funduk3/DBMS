package ru.open.cu.student.parser.nodes;


public class RangeTblEntry {
    public final String tableName;   // имя таблицы из каталога
    public String alias;             // псевдоним таблицы
    public int index;                // порядковый номер в запросе

    public RangeTblEntry(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be null or empty");
        }
        this.tableName = tableName;
        this.alias = null;
        this.index = 0;
    }

    @Override
    public String toString() {
        return "RangeTblEntry{" +
                "tableName='" + tableName + '\'' +
                ", alias='" + alias + '\'' +
                ", index=" + index +
                '}';
    }
}
