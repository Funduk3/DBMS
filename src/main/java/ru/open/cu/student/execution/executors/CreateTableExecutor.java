package ru.open.cu.student.execution.executors;


import ru.open.cu.student.catalog.manager.CatalogManager;
import ru.open.cu.student.catalog.model.TableDefinition;

public class CreateTableExecutor implements Executor {

    private final CatalogManager catalogManager;
    private final TableDefinition tableDefinition;

    public CreateTableExecutor(CatalogManager catalogManager, TableDefinition tableDefinition) {
        this.catalogManager = catalogManager;
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void open() { }

    @Override
    public Object next() {
        catalogManager.createTable(
                tableDefinition.getName(),
                tableDefinition.getColumns()
        );
        return null;
    }

    @Override
    public void close() { }
}