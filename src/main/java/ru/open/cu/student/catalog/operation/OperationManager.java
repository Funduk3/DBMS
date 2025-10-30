package ru.open.cu.student.catalog.operation;

import java.util.List;

public interface OperationManager {

    void insert(String tableName, Object... values);

    List<Object> select(String tableName, List<String> columnNames);

    List<List<Object>> selectAll(String table);
}
