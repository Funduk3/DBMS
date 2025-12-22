package ru.open.cu.student.parser.nodes;

public class ColumnRef implements Expr {
    public String table;       // имя таблицы (может быть null)
    public String column;      // имя столбца

    public ColumnRef(String table, String column) {
        this.table = table;
        this.column = column;
    }

    public ColumnRef(String column) {
        this(null, column);  // Только имя столбца, таблица не указана
    }

    @Override
    public String toString() {
        return table + " " + column;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }
}