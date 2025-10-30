package ru.open.cu.student.memory.buffer;

import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.replacer.Replacer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultBufferPoolManager implements BufferPoolManager {
    private final Map<Integer, BufferSlot> store;
    private final Replacer replacer;
    private final PageFileManager pageFileManager;
    private final int capacity;

    public DefaultBufferPoolManager(int poolSize, PageFileManager pageFileManager, Replacer replacer) {
        this.store = new HashMap<>();
        this.capacity = poolSize;
        this.replacer = replacer;
        this.pageFileManager = pageFileManager;
    }

    @Override
    public BufferSlot getPage(int pageId) throws IOException {
        BufferSlot bs = store.get(pageId);

        if (bs == null) {
            ensureFrame();
            Page page = pageFileManager.read(pageId, Path.of("data.heap"));
            bs = new BufferSlot(pageId, page);
            replacer.push(bs);
            store.put(pageId, bs);
        }

        bs.incrementUsage();
        return bs;
    }

    @Override
    public void updatePage(int pageId, Page page) {
        BufferSlot bs = store.get(pageId);
        if (bs == null) {
            throw new IllegalArgumentException("There is no such page in buffer: " + pageId);
        }
        bs.setPage(page);
        bs.setDirty(true);
        pageFileManager.write(page, Path.of("data.heap"));
    }

    @Override
    public void pinPage(int pageId) {
        BufferSlot desc = store.get(pageId);
        if (desc == null) {
            throw new IllegalArgumentException("Page not found in buffer: " + pageId);
        }

        replacer.delete(pageId);
        desc.setPinned(true);
    }

    private void ensureFrame() throws IOException {
        if (store.size() < capacity) return;

        BufferSlot victim = replacer.pickVictim();
        if (victim == null) {
            throw new IOException("No free frame: all pages are pinned");
        }

        store.remove(victim.getPageId());

        if (victim.isDirty()) {
            if (!(victim.getPage() instanceof HeapPage)) {
                throw new IOException("Dirty page is not a HeapPage: " + victim.getPageId());
            }
            pageFileManager.write(victim.getPage(), Path.of("data.heap"));
        }
    }

    @Override
    public void flushPage(int pageId) {
        BufferSlot bs = store.get(pageId);
        if (bs == null) {
            throw new IllegalArgumentException("Page not found in buffer: " + pageId);
        }
        if (!(bs.getPage() instanceof HeapPage)) {
            throw new IllegalArgumentException("Dirty page is not a HeapPage: " + bs.getPageId());
        }
        pageFileManager.write(bs.getPage(), Path.of("data.heap"));
        bs.setDirty(false);
    }

    @Override
    public void flushAllPages() {
        for (BufferSlot bs : getDirtyPages()) {
            if (!(bs.getPage() instanceof HeapPage)) {
                throw new IllegalArgumentException("Dirty page is not a HeapPage: " + bs.getPageId());
            }
            pageFileManager.write(bs.getPage(), Path.of("data.heap"));
            bs.setDirty(false);
        }
    }

    @Override
    public List<BufferSlot> getDirtyPages() {
        return store.values().stream().filter(BufferSlot::isDirty).toList();
    }
}
