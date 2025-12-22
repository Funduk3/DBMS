package ru.open.cu.student.catalog.operation;

import java.util.List;

public interface OperationManager {
    void insert(String table, Object... params);

    List<Object> select(String tableName, List<String> columnNames);

    List<List<Object>> selectAll(String table);

    List<Object> selectRowByTid(String tableName, ru.open.cu.student.index.TID tid);
}