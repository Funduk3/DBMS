package ru.open.cu.student.catalog.model;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TypeDefinition {
    private final int oid;
    private final String name;
    private final int byteLength;

    public TypeDefinition(int oid, String name, int byteLength) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name");
        this.byteLength = byteLength;
    }

    public int getOid() {
        return oid;
    }

    public String getName() {
        return name;
    }

    public int getByteLength() {
        return byteLength;
    }

    public byte[] toBytes() {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > Short.MAX_VALUE) {
            throw new IllegalStateException("type name is too long");
        }

        ByteBuffer buffer = ByteBuffer
                .allocate(4 + 4 + 2 + nameBytes.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(oid);
        buffer.putInt(byteLength);
        buffer.putShort((short) nameBytes.length);
        buffer.put(nameBytes);
        return buffer.array();
    }

    public static TypeDefinition fromBytes(byte[] raw) {
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < 4 + 4 + 2) {
            throw new IllegalArgumentException("payload is too small for TypeDefinition");
        }

        int oid = buffer.getInt();
        int byteLength = buffer.getInt();
        int nameLen = buffer.getShort() & 0xFFFF;
        if (buffer.remaining() != nameLen) {
            throw new IllegalArgumentException("invalid payload for TypeDefinition");
        }
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        String name = new String(nameBytes, StandardCharsets.UTF_8);
        return new TypeDefinition(oid, name, byteLength);
    }
}

