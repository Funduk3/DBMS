package ru.open.cu.student.memory.model;

import ru.open.cu.student.memory.page.Page;

public class BufferSlot {
    private final int pageId;
    private Page page;
    private boolean dirty;
    private boolean pinned;
    private int usageCount;
    private int fileId;

    public BufferSlot(int pageId, Page page) {
        this.pageId = pageId;
        this.page = page;
        this.dirty = false;
        this.pinned = false;
        this.usageCount = 0;
        this.fileId = 0; // по умолчанию data.heap
    }

    public int getPageId() {
        return pageId;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isPinned() {
        return pinned;
    }

    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    public int getUsageCount() {
        return usageCount;
    }

    public void incrementUsage() {
        this.usageCount++;
    }

    public void resetUsage() {
        this.usageCount = 0;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public String toString() {
        return "BufferSlot{" +
                "pageId=" + pageId +
                ", fileId=" + fileId +
                ", dirty=" + dirty +
                ", pinned=" + pinned +
                ", usageCount=" + usageCount +
                '}';
    }
}