package ru.open.cu.student.execution.executors;

import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.index.btree.BPlusTreeIndex;
import ru.open.cu.student.index.btree.BPlusTreeIndexImpl;
import ru.open.cu.student.index.hash.HashIndex;
import ru.open.cu.student.index.hash.HashIndexImpl;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.catalog.operation.OperationManager;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateIndexExecutor implements Executor {
    private final CatalogManagerImpl catalogManager;
    private final PageFileManager pageFileManager;
    private final OperationManager operationManager;
    private final String indexName;
    private final String tableName;
    private final String columnName;
    private final String indexType;

    public CreateIndexExecutor(CatalogManagerImpl catalogManager,
                              PageFileManager pageFileManager,
                              OperationManager operationManager,
                              String indexName,
                              String tableName,
                              String columnName,
                              String indexType) {
        this.catalogManager = catalogManager;
        this.pageFileManager = pageFileManager;
        this.operationManager = operationManager;
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.indexType = indexType;
    }

    @Override
    public void open() {
        // Ничего не нужно инициализировать
    }

    @Override
    public Object next() {
        // Проверяем, что таблица существует
        TableDefinition table = catalogManager.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exist");
        }

        // Проверяем, что индекс с таким именем еще не существует
        if (catalogManager.getIndex(indexName) != null) {
            throw new IllegalArgumentException("Index '" + indexName + "' already exists");
        }

        // Создаем индекс в зависимости от типа
        Index index;
        Path indexPath;

        if ("HASH".equals(indexType)) {
            indexPath = Paths.get("data", indexName + "_hash");
            index = new HashIndexImpl(
                indexName,
                columnName,
                pageFileManager,
                operationManager,
                indexPath
            );
        } else if ("BTREE".equals(indexType)) {
            indexPath = Paths.get("data", indexName + "_btree");
            index = new BPlusTreeIndexImpl(
                indexName,
                columnName,
                4,  // order
                pageFileManager,
                indexPath
            );
        } else {
            throw new IllegalArgumentException("Invalid index type: " + indexType + ". Expected HASH or BTREE");
        }

        // Регистрируем индекс в каталоге
        catalogManager.registerIndex(index);

        return null;
    }

    @Override
    public void close() {
        // Ничего не нужно закрывать
    }
}

