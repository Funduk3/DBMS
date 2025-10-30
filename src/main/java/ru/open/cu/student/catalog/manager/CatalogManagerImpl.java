package ru.open.cu.student.catalog.manager;

import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.memory.manager.PageFileManager;
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

    private final PageFileManager pageFileManager;

    private final Map<Integer, TableDefinition> tablesById = new ConcurrentHashMap<>();
    private final Map<String, TableDefinition> tablesByName = new ConcurrentHashMap<>();
    private final Map<Integer, List<ColumnDefinition>> columnsByTableOid = new ConcurrentHashMap<>();
    private final Map<Integer, TypeDefinition> typesById = new ConcurrentHashMap<>();

    private final Path catalogPath;
    private final Path dataPath;

    public CatalogManagerImpl(PageFileManager pageFileManager) {
        this.pageFileManager = pageFileManager;
        this.catalogPath = Paths.get(CATALOG_DIR);
        this.dataPath = Paths.get(DATA_DIR);

        try {
            Files.createDirectories(catalogPath);
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories", e);
        }

        initializeTypes();
        loadAll();
    }

    private void initializeTypes() {
        Path typePath = catalogPath.resolve(TYPE_DEFS_FILE);

        try {
            pageFileManager.read(0, typePath);
        } catch (Exception e) {
            typesById.put(1, new TypeDefinition(1, "INT64", 8));
            typesById.put(2, new TypeDefinition(2, "VARCHAR", -1));
            saveTypes();
        }
    }

    public synchronized TableDefinition createTable(TableDefinition tableDefinition, List<ColumnDefinition> columns) {
        if (tablesByName.containsKey(tableDefinition.getName())) {
            throw new IllegalArgumentException("Table already exists: " + tableDefinition.getName());
        }

        for (ColumnDefinition column : columns) {
            var c = column.getTypeOid();
            if (!typesById.containsKey(column.getTypeOid())) {
                throw new IllegalArgumentException("Type not found: " + column.getTypeOid());
            }
        }

        tablesById.put(tableDefinition.getOid(), tableDefinition);
        tablesByName.put(tableDefinition.getName(), tableDefinition);

        List<ColumnDefinition> tableColumns = new ArrayList<>(columns);
        columnsByTableOid.put(tableDefinition.getOid(), tableColumns);

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
            ColumnDefinition srcColumn = columns.get(i);
            ColumnDefinition newColumn = new ColumnDefinition(
                    i,
                    tableOid,
                    srcColumn.getTypeOid(),
                    srcColumn.getName(),
                    i
            );
            tableColumns.add(newColumn);
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
            throw new RuntimeException("Failed to create data file for table: " + table.getName(), e);
        }
    }

    @Override
    public TableDefinition getTable(String tableName) {
        TableDefinition table = tablesByName.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return table;
    }

    @Override
    public ColumnDefinition getColumn(TableDefinition table, String columnName) {
        List<ColumnDefinition> columns = columnsByTableOid.get(table.getOid());
        if (columns == null) {
            throw new IllegalArgumentException("No columns found for table: " + table.getName());
        }

        for (ColumnDefinition column : columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }

        throw new IllegalArgumentException("Column not found: " + columnName + " in table " + table.getName());
    }

    @Override
    public List<TableDefinition> listTables() {
        return new ArrayList<>(tablesByName.values());
    }

    @Override
    public TypeDefinition getTypeByName(String name) {
        for (Map.Entry<Integer, TypeDefinition> entry : typesById.entrySet()) {
            TypeDefinition typeDefinition = entry.getValue();
            if (typeDefinition.getName().equals(name)) {
                return typeDefinition;
            }
        }
        return null;
    }

    public TypeDefinition getType(int typeOid) {
        TypeDefinition type = typesById.get(typeOid);
        if (type == null) {
            throw new IllegalArgumentException("Type not found: " + typeOid);
        }
        return type;
    }

    public List<ColumnDefinition> getTableColumns(int oid) {
        List<ColumnDefinition> columns = columnsByTableOid.get(oid);
        if (columns == null) {
            throw new IllegalArgumentException("No columns found for table OID: " + oid);
        }
        return new ArrayList<>(columns);
    }

    public Path getDataFilePath(TableDefinition table) {
        return dataPath.resolve(table.getFileNode());
    }

    /**
     * Загрузка всех метаданных при старте системы
     */
    private void loadAll() {
        loadTypes();
        loadTables();
        loadColumns();
    }

    private void loadTables() {
        tablesById.clear();
        tablesByName.clear();

        Path tablePath = catalogPath.resolve(TABLE_DEFS_FILE);
        int pageId = 0;

        while (true) {
            try {
                Page page = pageFileManager.read(pageId, tablePath);

                for (int i = 0; i < page.size(); i++) {
                    byte[] data = page.read(i);
                    TableDefinition table = TableDefinition.fromBytes(data);
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

        Path typePath = catalogPath.resolve(TYPE_DEFS_FILE);
        int pageId = 0;

        while (true) {
            try {
                Page page = pageFileManager.read(pageId, typePath);

                for (int i = 0; i < page.size(); i++) {
                    byte[] data = page.read(i);
                    TypeDefinition type = TypeDefinition.fromBytes(data);
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

        Path columnPath = catalogPath.resolve(COLUMN_DEFS_FILE);
        int pageId = 0;

        while (true) {
            try {
                Page page = pageFileManager.read(pageId, columnPath);

                for (int i = 0; i < page.size(); i++) {
                    byte[] data = page.read(i);
                    ColumnDefinition column = ColumnDefinition.fromBytes(data);

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
        Path tablePath = catalogPath.resolve(TABLE_DEFS_FILE);
        List<TableDefinition> tables = new ArrayList<>(tablesById.values());

        saveObjectsToPages(tablePath, tables, TableDefinition::toBytes);
    }

    private void saveTypes() {
        Path typePath = catalogPath.resolve(TYPE_DEFS_FILE);
        List<TypeDefinition> types = new ArrayList<>(typesById.values());

        saveObjectsToPages(typePath, types, TypeDefinition::toBytes);
    }

    private void saveColumns() {
        Path columnPath = catalogPath.resolve(COLUMN_DEFS_FILE);
        List<ColumnDefinition> allColumns = new ArrayList<>();

        for (List<ColumnDefinition> columns : columnsByTableOid.values()) {
            allColumns.addAll(columns);
        }

        saveObjectsToPages(columnPath, allColumns, ColumnDefinition::toBytes);
    }

    private <T> void saveObjectsToPages(Path filePath, List<T> objects,
                                        java.util.function.Function<T, byte[]> serializer) {
        int pageId = 0;
        Page currentPage = new HeapPage(pageId);

        for (T obj : objects) {
            byte[] data = serializer.apply(obj);

            try {
                currentPage.write(data);
            } catch (IllegalArgumentException e) {
                pageFileManager.write(currentPage, filePath);
                pageId++;
                currentPage = new HeapPage(pageId);
                currentPage.write(data);
            }
        }

        if (currentPage.size() > 0) {
            pageFileManager.write(currentPage, filePath);
        }
    }
}