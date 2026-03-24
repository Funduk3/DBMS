package ru.open.cu.student.catalog.operation;

import ru.open.cu.student.catalog.manager.CatalogManagerImpl;
import ru.open.cu.student.catalog.model.ColumnDefinition;
import ru.open.cu.student.catalog.model.TableDefinition;
import ru.open.cu.student.catalog.model.TypeDefinition;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class OperationManagerImpl implements OperationManager {

    private final CatalogManagerImpl catalogManager;
    private final PageFileManager pageFileManager;

    public OperationManagerImpl(CatalogManagerImpl catalogManager, PageFileManager pageFileManager) {
        this.catalogManager = catalogManager;
        this.pageFileManager = pageFileManager;
    }

    @Override
    public void insert(String table, Object... params) {
        TableDefinition tableDef = catalogManager.getTable(table);
        List<ColumnDefinition> columns = catalogManager.getTableColumns(tableDef.getOid());

        if (params.length != columns.size()) {
            throw new IllegalArgumentException(
                    String.format("Values count (%d) doesn't match columns count (%d)",
                            params.length, columns.size())
            );
        }

        byte[] rowData = serializeRow(columns, params);

        Path dataFile = catalogManager.getDataFilePath(tableDef);
        Page page = findPageForInsert(dataFile, tableDef, rowData.length);

        page.write(rowData);
        pageFileManager.write(page, dataFile);

        int newPagesCount = page.getPageId() + 1;
        if (newPagesCount > tableDef.getPagesCount()) {
            tableDef.setPagesCount(newPagesCount);
            catalogManager.updateTableMetadata(tableDef);
        }
    }

    public List<List<Object>> selectAll(String table) {
        TableDefinition tableDef = catalogManager.getTable(table);
        List<ColumnDefinition> columns = catalogManager.getTableColumns(tableDef.getOid());

        List<List<Object>> result = new ArrayList<>();
        Path dataFile = catalogManager.getDataFilePath(tableDef);

        for (int pageId = 0; pageId < tableDef.getPagesCount(); pageId++) {
            try {
                Page page = pageFileManager.read(pageId, dataFile);
                for (int i = 0; i < page.size(); i++) {
                    byte[] rowData = page.read(i);
                    if (rowData == null || rowData.length == 0) {
                        continue;
                    }
                    if (isEmptyRecord(rowData)) {
                        continue;
                    }
                    try {
                        List<Object> rowValues = deserializeRow(columns, rowData);
                        result.add(rowValues);
                    } catch (Exception e) {
                        continue;
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }
        return result;
    }

    private boolean isEmptyRecord(byte[] data) {
        if (data == null || data.length == 0) {
            return true;
        }

        for (byte b : data) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<Object> select(String tableName, List<String> columnNames) {
        TableDefinition table = catalogManager.getTable(tableName);
        List<ColumnDefinition> allColumns = catalogManager.getTableColumns(table.getOid());

        List<ColumnDefinition> selectedColumns;
        if (columnNames == null || columnNames.isEmpty()) {
            selectedColumns = allColumns;
        } else {
            selectedColumns = new ArrayList<>();
            for (String colName : columnNames) {
                ColumnDefinition col = catalogManager.getColumn(table, colName);
                selectedColumns.add(col);
            }
        }

        List<Object> result = new ArrayList<>();
        Path dataFile = catalogManager.getDataFilePath(table);

        for (int pageId = 0; pageId < table.getPagesCount(); pageId++) {
            try {
                Page page = pageFileManager.read(pageId, dataFile);
                for (int i = 0; i < page.size(); i++) {
                    byte[] rowData = page.read(i);
                    if (rowData == null || rowData.length == 0 || isEmptyRecord(rowData)) {
                        continue;
                    }
                    try {
                        List<Object> rowValues = deserializeRow(allColumns, rowData);
                        if (columnNames != null && !columnNames.isEmpty()) {
                            List<Object> filteredValues = new ArrayList<>();
                            for (ColumnDefinition col : selectedColumns) {
                                filteredValues.add(rowValues.get(col.getPosition()));
                            }
                            result.add(filteredValues);
                        } else {
                            result.add(rowValues);
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }
        return result;
    }

    @Override
    public List<Object> selectRowByTid(String tableName, TID tid) {
        TableDefinition tableDef = catalogManager.getTable(tableName);
        List<ColumnDefinition> columns = catalogManager.getTableColumns(tableDef.getOid());

        Path dataFile = catalogManager.getDataFilePath(tableDef);

        try {
            Page page = pageFileManager.read(tid.pageId(), dataFile);

            if (!page.isValid() || tid.slotId() >= page.size()) {
                return null;
            }

            byte[] rowData = page.read(tid.slotId());

            return deserializeRow(columns, rowData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read row by TID: " + tid, e);
        }
    }

    private byte[] serializeRow(List<ColumnDefinition> columns, Object[] values) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            for (int i = 0; i < columns.size(); i++) {
                ColumnDefinition column = columns.get(i);
                Object value = values[i];
                TypeDefinition type = catalogManager.getType(column.getTypeOid());

                serializeValue(dos, type, value);
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }

    private void serializeValue(DataOutputStream dos, TypeDefinition type, Object value) throws IOException {
        if (value == null) {
            dos.writeBoolean(true);
            return;
        }

        dos.writeBoolean(false);

        switch (type.getName()) {
            case "INT":
            case "INT64":
                if (value instanceof Number) {
                    dos.writeLong(((Number) value).longValue());
                } else {
                    throw new IllegalArgumentException("Expected number for " + type.getName() + ", got: " + value.getClass());
                }
                break;

            case "VARCHAR":
                String strValue = value.toString();
                byte[] bytes = strValue.getBytes(StandardCharsets.UTF_8);
                dos.writeInt(bytes.length);
                dos.write(bytes);
                break;

            default:
                if (type.getName().startsWith("VARCHAR_")) {
                    String str = value.toString();
                    byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(strBytes.length);
                    dos.write(strBytes);
                } else {
                    throw new IllegalArgumentException("Unknown type: " + type.getName());
                }
        }
    }

    private List<Object> deserializeRow(List<ColumnDefinition> columns, byte[] rowData) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(rowData);
             DataInputStream dis = new DataInputStream(bais)) {

            List<Object> values = new ArrayList<>();

            for (ColumnDefinition column : columns) {
                TypeDefinition type = catalogManager.getType(column.getTypeOid());
                Object value = deserializeValue(dis, type);
                values.add(value);
            }

            return values;
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize row", e);
        }
    }

    private Object deserializeValue(DataInputStream dis, TypeDefinition type) throws IOException {
        boolean isNull = dis.readBoolean();
        if (isNull) {
            return null;
        }

        switch (type.getName()) {
            case "INT":
            case "INT64":
                return dis.readLong();

            case "VARCHAR":
                int length = dis.readInt();
                byte[] bytes = new byte[length];
                dis.readFully(bytes);
                return new String(bytes, StandardCharsets.UTF_8);

            default:
                if (type.getName().startsWith("VARCHAR_")) {
                    int len = dis.readInt();
                    byte[] strBytes = new byte[len];
                    dis.readFully(strBytes);
                    return new String(strBytes, StandardCharsets.UTF_8);
                } else {
                    throw new IllegalArgumentException("Unknown type: " + type.getName());
                }
        }
    }

    private Page findPageForInsert(Path dataFile, TableDefinition table, int dataSize) {
        for (int pageId = 0; pageId < table.getPagesCount(); pageId++) {
            try {
                Page page = pageFileManager.read(pageId, dataFile);
                if (hasSpaceForData(page, dataSize)) {
                    return page;
                }
            } catch (Exception e) {
                continue;
            }
        }

        int newPageId = table.getPagesCount();
        return new HeapPage(newPageId);
    }

    private boolean hasSpaceForData(Page page, int dataSize) {
        try {
            byte[] testData = new byte[dataSize];
            HeapPage heapPage = new HeapPage(page.getPageId(), page.bytes());
            heapPage.write(testData);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}