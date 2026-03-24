package ru.open.cu.student.memory.page;

import java.nio.ByteBuffer;

public class HeapPage implements Page {

    public static final int PAGE_SIZE = 8192;
    private static final int HEADER_SIZE = 10;

    private final ByteBuffer data;
    private final int pageId;

    public HeapPage(int pageId, byte[] data) {
        this.data = ByteBuffer.wrap(data);
        this.pageId = pageId;
    }

    public HeapPage(int pageId) {
        this.data = ByteBuffer.allocate(PAGE_SIZE);
        this.pageId = pageId;

        data.putInt(0, 0xDBDB01);
        data.putShort(4, (short) 0);
        data.putShort(6, (short) HEADER_SIZE);
        data.putShort(8, (short) PAGE_SIZE);
    }

    @Override
    public byte[] bytes() {
        return data.array();
    }

    @Override
    public int getPageId() {
        return pageId;
    }

    @Override
    public int size() {
        return data.getShort(4) & 0xFFFF;
    }

    private int lower() {
        return data.getShort(6) & 0xFFFF;
    }

    private int upper() {
        return data.getShort(8) & 0xFFFF;
    }

    public boolean isSlotValid(int index) {
        if (index < 0 || index >= size()) {
            return false;
        }
        var length = data.getShort(HEADER_SIZE + index * 4 + 2) & 0xFFFF;
        return length > 0;
    }

    @Override
    public boolean isValid() {
        return data.getInt(0) == 0xDBDB01;
    }

    @Override
    public byte[] read(int index) {
        var offset = data.getShort(HEADER_SIZE + index * 4) & 0xFFFF;
        var length = data.getShort(HEADER_SIZE + index * 4 + 2) & 0xFFFF;
        if (length == 0) {
            return null;
        }
        var result = new byte[length];
        data.get(offset, result);
        return result;
    }

    @Override
    public void write(byte[] data) {
        var lower = lower();
        var upper = upper();
        var index = size();

        if (upper - lower < data.length + 4) {
            throw new IllegalArgumentException("Not enough space");
        }

        this.data.put(upper - data.length, data);
        this.data.putShort(6, (short) (lower + 4));
        this.data.putShort(8, (short) (upper - data.length));
        this.data.putShort(HEADER_SIZE + index * 4, (short) (upper - data.length));
        this.data.putShort(HEADER_SIZE + index * 4 + 2, (short) data.length);
        this.data.putShort(4, (short) (index + 1));
    }
}
