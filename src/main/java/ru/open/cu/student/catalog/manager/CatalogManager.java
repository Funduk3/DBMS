package ru.open.cu.student.catalog.manager;

import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;

import java.util.List;

public interface CatalogManager {

    TableDefinition createTable(String name, List<ColumnDefinition> columns);

    TableDefinition getTable(String tableName);

    ColumnDefinition getColumn(TableDefinition table, String columnName);

    List<TableDefinition> listTables();

    TypeDefinition getTypeByName(String name);

    TypeDefinition getType(int typeOid);

    List<ColumnDefinition> getTableColumns(int oid);

    Object getIndex(String indexName);

    /**
     * Найти индекс для указанной таблицы и колонки
     * @param tableName имя таблицы
     * @param columnName имя колонки
     * @return индекс или null, если индекс не найден
     */
    Object findIndexByTableAndColumn(String tableName, String columnName);
}
