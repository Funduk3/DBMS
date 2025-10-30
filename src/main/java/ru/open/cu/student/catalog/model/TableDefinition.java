package ru.open.cu.student.catalog.model;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TableDefinition {
    private final int oid;
    private final String name;
    private final String type;
    private final String fileNode;
    private int pagesCount;
    private final List<ColumnDefinition> columns = new ArrayList<>();

    public TableDefinition(int oid, String name, String type, String fileNode, int pagesCount) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.fileNode = Objects.requireNonNull(fileNode, "fileNode");
        this.pagesCount = pagesCount;
    }

    private TableDefinition(int oid, String name, String type, String fileNode, int pagesCount, List<ColumnDefinition> columns) {
        this(oid, name, type, fileNode, pagesCount);
        this.columns.addAll(columns);
    }

    public int getOid() {
        return oid;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getFileNode() {
        return fileNode;
    }

    public int getPagesCount() {
        return pagesCount;
    }

    public void setPagesCount(int pagesCount) {
        this.pagesCount = pagesCount;
    }

    public List<ColumnDefinition> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    // Заменяем список колонок, сохраняя неизменяемый вид наружу.
    public void setColumns(List<ColumnDefinition> columnDefinitions) {
        columns.clear();
        columns.addAll(columnDefinitions);
    }

    public byte[] toBytes() {
        byte[] nameBytes = toBytes(name);
        byte[] typeBytes = toBytes(type);
        byte[] fileNodeBytes = toBytes(fileNode);

        ByteBuffer buffer = ByteBuffer
                .allocate(4 + 4 + 2 + nameBytes.length + 2 + typeBytes.length + 2 + fileNodeBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);

        buffer.putInt(oid);
        buffer.putInt(pagesCount);
        buffer.putShort((short) nameBytes.length);
        buffer.put(nameBytes);
        buffer.putShort((short) typeBytes.length);
        buffer.put(typeBytes);
        buffer.putShort((short) fileNodeBytes.length);
        buffer.put(fileNodeBytes);
        return buffer.array();
    }

    private static String readString(ByteBuffer buffer) {
        int len = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() < len) {
            throw new IllegalArgumentException("invalid string payload");
        }
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static TableDefinition fromBytes(byte[] raw) {
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < 4 + 4 + 2 + 2 + 2) {
            throw new IllegalArgumentException("payload is too small for TableDefinition");
        }

        int oid = buffer.getInt();
        int pagesCount = buffer.getInt();
        String name = readString(buffer);
        String type = readString(buffer);
        String fileNode = readString(buffer);
        return new TableDefinition(oid, name, type, fileNode, pagesCount);
    }

    private static byte[] toBytes(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new IllegalStateException("too long string");
        }
        return bytes;
    }
}
