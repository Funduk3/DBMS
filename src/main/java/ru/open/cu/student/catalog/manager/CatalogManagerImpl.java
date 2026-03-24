package ru.open.cu.student.catalog.manager;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.Index;
import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogManagerImpl implements CatalogManager {

    private static final String CATALOG_DIR = "catalog";
    private static final String DATA_DIR = "data";
    private static final String TABLE_DEFS_FILE = "table_definitions.dat";
    private static final String COLUMN_DEFS_FILE = "column_definitions.dat";
    private static final String TYPE_DEFS_FILE = "type_definitions.dat";

    private final DefaultBufferPoolManager bufferPoolManager;
    private final PageFileManager pageFileManager;

    private final Map<Integer, TableDefinition> tablesById = new ConcurrentHashMap<>();
    private final Map<String, TableDefinition> tablesByName = new ConcurrentHashMap<>();
    private final Map<Integer, List<ColumnDefinition>> columnsByTableOid = new ConcurrentHashMap<>();
    private final Map<Integer, TypeDefinition> typesById = new ConcurrentHashMap<>();
    private final Map<String, Index> indexesByName = new ConcurrentHashMap<>();

    private final Path catalogPath;
    private final Path dataPath;

    private final int tableDefsFileId;
    private final int columnDefsFileId;
    private final int typeDefsFileId;

    public CatalogManagerImpl(DefaultBufferPoolManager bufferPoolManager, PageFileManager pageFileManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.pageFileManager = pageFileManager;
        this.catalogPath = Paths.get(CATALOG_DIR);
        this.dataPath = Paths.get(DATA_DIR);

        try {
            Files.createDirectories(catalogPath);
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.tableDefsFileId = bufferPoolManager.registerFile(catalogPath.resolve(TABLE_DEFS_FILE));
        this.columnDefsFileId = bufferPoolManager.registerFile(catalogPath.resolve(COLUMN_DEFS_FILE));
        this.typeDefsFileId = bufferPoolManager.registerFile(catalogPath.resolve(TYPE_DEFS_FILE));

        initializeTypes();
        loadAll();
    }

    private void initializeTypes() {
        Path typePath = catalogPath.resolve(TYPE_DEFS_FILE);

        if (!Files.exists(typePath)) {
            typesById.put(1, new TypeDefinition(1, "INT", 8));
            typesById.put(2, new TypeDefinition(2, "VARCHAR", -1));
            saveTypes();
        }
    }

    public synchronized TableDefinition createTable(TableDefinition tableDefinition, List<ColumnDefinition> columns) {
        if (tablesByName.containsKey(tableDefinition.getName())) {
            throw new IllegalArgumentException();
        }

        for (ColumnDefinition column : columns) {
            if (!typesById.containsKey(column.getTypeOid())) {
                throw new IllegalArgumentException();
            }
        }

        tablesById.put(tableDefinition.getOid(), tableDefinition);
        tablesByName.put(tableDefinition.getName(), tableDefinition);

        List<ColumnDefinition> tableColumns = new ArrayList<>(columns);
        columnsByTableOid.put(tableDefinition.getOid(), tableColumns);
        tableDefinition.setColumns(new ArrayList<>(tableColumns));

        createEmptyDataFile(tableDefinition);
        saveAll();

        return tableDefinition;
    }

    @Override
    public synchronized TableDefinition createTable(String name, List<ColumnDefinition> columns) {
        int maxOid = tablesById.keySet().stream().max(Integer::compareTo).orElse(0);
        int tableOid = maxOid + 1;

        String fileNode = tableOid + ".dat";
        TableDefinition table = new TableDefinition(tableOid, name, "TABLE", fileNode, 0);

        List<ColumnDefinition> tableColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition src = columns.get(i);
            tableColumns.add(new ColumnDefinition(
                    i,
                    tableOid,
                    src.getTypeOid(),
                    src.getName(),
                    i
            ));
        }

        return createTable(table, tableColumns);
    }

    private void createEmptyDataFile(TableDefinition table) {
        Path dataFile = dataPath.resolve(table.getFileNode());

        try {
            if (!Files.exists(dataFile)) {
                Page emptyPage = new HeapPage(0);
                pageFileManager.write(emptyPage, dataFile);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableDefinition getTable(String tableName) {
        return tablesByName.get(tableName);
    }

    @Override
    public ColumnDefinition getColumn(TableDefinition table, String columnName) {
        List<ColumnDefinition> columns = columnsByTableOid.get(table.getOid());
        if (columns == null) {
            throw new IllegalArgumentException();
        }

        for (ColumnDefinition column : columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }

        throw new IllegalArgumentException();
    }

    @Override
    public List<TableDefinition> listTables() {
        return new ArrayList<>(tablesByName.values());
    }

    @Override
    public TypeDefinition getTypeByName(String name) {
        for (TypeDefinition type : typesById.values()) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        return null;
    }

    public TypeDefinition getType(int typeOid) {
        TypeDefinition type = typesById.get(typeOid);
        if (type == null) {
            throw new IllegalArgumentException();
        }
        return type;
    }

    public List<ColumnDefinition> getTableColumns(int oid) {
        List<ColumnDefinition> columns = columnsByTableOid.get(oid);
        if (columns == null) {
            throw new IllegalArgumentException();
        }
        return new ArrayList<>(columns);
    }

    public synchronized void registerIndex(Index index) {
        if (indexesByName.containsKey(index.getName())) {
            throw new IllegalArgumentException();
        }
        indexesByName.put(index.getName(), index);
    }

    @Override
    public Index getIndex(String indexName) {
        return indexesByName.get(indexName);
    }

    public synchronized void dropIndex(String indexName) {
        indexesByName.remove(indexName);
    }

    public List<Index> listIndexes() {
        return new ArrayList<>(indexesByName.values());
    }

    @Override
    public Index findIndexByTableAndColumn(String tableName, String columnName) {
        for (Index index : indexesByName.values()) {
            if (index.getColumnName().equals(columnName)) {
                // Проверяем, что индекс относится к нужной таблице
                // Для этого нужно проверить, что таблица существует и содержит эту колонку
                TableDefinition table = getTable(tableName);
                if (table != null) {
                    try {
                        ColumnDefinition column = getColumn(table, columnName);
                        if (column != null) {
                            return index;
                        }
                    } catch (IllegalArgumentException e) {
                        // Колонка не найдена в таблице
                        continue;
                    }
                }
            }
        }
        return null;
    }

    public Path getDataFilePath(TableDefinition table) {
        return dataPath.resolve(table.getFileNode());
    }

    private void loadAll() {
        loadTypes();
        loadTables();
        loadColumns();
    }

    private void loadTables() {
        tablesById.clear();
        tablesByName.clear();

        int pageId = 0;
        while (true) {
            try {
                BufferSlot slot = bufferPoolManager.getPage(tableDefsFileId, pageId);
                Page page = slot.getPage();

                for (int i = 0; i < page.size(); i++) {
                    TableDefinition table = TableDefinition.fromBytes(page.read(i));
                    tablesById.put(table.getOid(), table);
                    tablesByName.put(table.getName(), table);
                }
                pageId++;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void loadTypes() {
        typesById.clear();

        int pageId = 0;
        while (true) {
            try {
                BufferSlot slot = bufferPoolManager.getPage(typeDefsFileId, pageId);
                Page page = slot.getPage();

                for (int i = 0; i < page.size(); i++) {
                    TypeDefinition type = TypeDefinition.fromBytes(page.read(i));
                    typesById.put(type.getOid(), type);
                }
                pageId++;
            } catch (Exception e) {
                break;
            }
        }
    }

    private void loadColumns() {
        columnsByTableOid.clear();

        int pageId = 0;
        while (true) {
            try {
                BufferSlot slot = bufferPoolManager.getPage(columnDefsFileId, pageId);
                Page page = slot.getPage();

                for (int i = 0; i < page.size(); i++) {
                    ColumnDefinition column = ColumnDefinition.fromBytes(page.read(i));
                    columnsByTableOid
                            .computeIfAbsent(column.getTableOid(), k -> new ArrayList<>())
                            .add(column);
                }
                pageId++;
            } catch (Exception e) {
                break;
            }
        }

        for (List<ColumnDefinition> columns : columnsByTableOid.values()) {
            columns.sort(Comparator.comparingInt(ColumnDefinition::getPosition));
        }
    }

    public synchronized void updateTableMetadata(TableDefinition table) {
        tablesById.put(table.getOid(), table);
        tablesByName.put(table.getName(), table);
        saveTables();
    }

    private void saveAll() {
        saveTables();
        saveTypes();
        saveColumns();
    }

    private void saveTables() {
        saveObjectsToPages(
                tableDefsFileId,
                new ArrayList<>(tablesById.values()),
                TableDefinition::toBytes
        );
    }

    private void saveTypes() {
        saveObjectsToPages(
                typeDefsFileId,
                new ArrayList<>(typesById.values()),
                TypeDefinition::toBytes
        );
    }

    private void saveColumns() {
        List<ColumnDefinition> allColumns = new ArrayList<>();
        for (List<ColumnDefinition> cols : columnsByTableOid.values()) {
            allColumns.addAll(cols);
        }
        saveObjectsToPages(columnDefsFileId, allColumns, ColumnDefinition::toBytes);
    }

    private <T> void saveObjectsToPages(int fileId, List<T> objects,
                                        java.util.function.Function<T, byte[]> serializer) {
        int pageId = 0;
        Page currentPage = new HeapPage(pageId);

        for (T obj : objects) {
            byte[] data = serializer.apply(obj);
            try {
                currentPage.write(data);
            } catch (IllegalArgumentException e) {
                writePage(fileId, pageId, currentPage);
                pageId++;
                currentPage = new HeapPage(pageId);
                currentPage.write(data);
            }
        }

        if (currentPage.size() > 0) {
            writePage(fileId, pageId, currentPage);
        }
    }

    private void writePage(int fileId, int pageId, Page page) {
        try {
            try {
                bufferPoolManager.getPage(fileId, pageId);
                bufferPoolManager.updatePage(fileId, pageId, page);
            } catch (IOException e) {
                bufferPoolManager.newPage(fileId, pageId);
                bufferPoolManager.updatePage(fileId, pageId, page);
            }
            bufferPoolManager.flushPage(fileId, pageId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
