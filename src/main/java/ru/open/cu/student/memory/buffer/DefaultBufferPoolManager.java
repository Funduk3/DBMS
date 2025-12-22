package ru.open.cu.student.memory.buffer;

import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.model.BufferSlot;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;
import ru.open.cu.student.memory.replacer.Replacer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.open.cu.student.memory.page.HeapPage.PAGE_SIZE;

public class DefaultBufferPoolManager implements BufferPoolManager {
    private final Map<BufferTag, BufferSlot> store;
    private final Map<Integer, Path> fileRegistry;
    private final Replacer replacer;
    private final PageFileManager pageFileManager;
    private final int capacity;
    private final AtomicInteger nextFileId = new AtomicInteger(1);

    public DefaultBufferPoolManager(int poolSize, PageFileManager pageFileManager, Replacer replacer) {
        this.store = new HashMap<>();
        this.fileRegistry = new ConcurrentHashMap<>();
        this.capacity = poolSize;
        this.replacer = replacer;
        this.pageFileManager = pageFileManager;
    }

    /**
     * Регистрирует файл и возвращает его ID
     */
    public int registerFile(Path filePath) {
        Path normalized = filePath.normalize();

        // Ищем существующий файл
        for (Map.Entry<Integer, Path> entry : fileRegistry.entrySet()) {
            if (entry.getValue().equals(normalized)) {
                return entry.getKey();
            }
        }

        // Регистрируем новый файл
        int fileId = nextFileId.getAndIncrement();
        fileRegistry.put(fileId, normalized);
        return fileId;
    }

    /**
     * Получает путь к файлу по его ID
     */
    private Path getFilePath(int fileId) {
        if (fileId == 0) {
            return Path.of("data.heap");
        }
        Path path = fileRegistry.get(fileId);
        if (path == null) {
            throw new IllegalArgumentException("File not registered: " + fileId);
        }
        return path;
    }

    @Override
    public BufferSlot getPage(int pageId) throws IOException {
        return getPage(0, pageId);
    }

    /**
     * Получает страницу из буфера или загружает с диска
     * Если файл не существует или страница за пределами файла - выбрасывает исключение
     */
    public BufferSlot getPage(int fileId, int pageId) throws IOException {
        BufferTag tag = new BufferTag(fileId, pageId);
        BufferSlot bs = store.get(tag);

        if (bs == null) {
            ensureFrame();

            Path filePath = getFilePath(fileId);

            // Проверяем существование файла и размер
            if (!Files.exists(filePath)) {
                throw new IOException("File does not exist: " + filePath);
            }

            long fileSize = Files.size(filePath);
            long requiredOffset = ((long) pageId) * PAGE_SIZE;

            if (fileSize < requiredOffset + PAGE_SIZE) {
                throw new IOException("Page " + pageId + " is out of file bounds for " + filePath);
            }

            Page page = pageFileManager.read(pageId, filePath);

            bs = new BufferSlot(pageId, page);
            bs.setFileId(fileId);
            replacer.push(bs);
            store.put(tag, bs);
        }

        bs.incrementUsage();
        return bs;
    }

    /**
     * Создаёт новую страницу в файле и добавляет её в буфер
     * Используется для расширения файлов
     */
    public BufferSlot newPage(int fileId, int pageId) throws IOException {
        BufferTag tag = new BufferTag(fileId, pageId);

        if (store.containsKey(tag)) {
            throw new IllegalArgumentException("Page already exists in buffer: fileId=" + fileId + ", pageId=" + pageId);
        }

        ensureFrame();

        Path filePath = getFilePath(fileId);
        Page newPage = new HeapPage(pageId);

        // Записываем новую страницу на диск
        pageFileManager.write(newPage, filePath);

        // Добавляем в буфер
        BufferSlot bs = new BufferSlot(pageId, newPage);
        bs.setFileId(fileId);
        bs.setDirty(false); // Уже записана на диск
        replacer.push(bs);
        store.put(tag, bs);

        bs.incrementUsage();
        return bs;
    }

    @Override
    public void updatePage(int pageId, Page page) {
        updatePage(0, pageId, page);
    }

    public void updatePage(int fileId, int pageId, Page page) {
        BufferTag tag = new BufferTag(fileId, pageId);
        BufferSlot bs = store.get(tag);
        if (bs == null) {
            throw new IllegalArgumentException("There is no such page in buffer: fileId=" + fileId + ", pageId=" + pageId);
        }
        bs.setPage(page);
        bs.setDirty(true);
    }

    @Override
    public void pinPage(int pageId) {
        pinPage(0, pageId);
    }

    public void pinPage(int fileId, int pageId) {
        BufferTag tag = new BufferTag(fileId, pageId);
        BufferSlot desc = store.get(tag);
        if (desc == null) {
            throw new IllegalArgumentException("Page not found in buffer: fileId=" + fileId + ", pageId=" + pageId);
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

        int victimFileId = victim.getFileId();
        Path victimPath = getFilePath(victimFileId);

        BufferTag victimTag = new BufferTag(victimFileId, victim.getPageId());
        store.remove(victimTag);

        if (victim.isDirty()) {
            if (!(victim.getPage() instanceof HeapPage)) {
                throw new IOException("Dirty page is not a HeapPage: " + victim.getPageId());
            }
            pageFileManager.write(victim.getPage(), victimPath);
        }
    }

    @Override
    public void flushPage(int pageId) {
        flushPage(0, pageId);
    }

    public void flushPage(int fileId, int pageId) {
        BufferTag tag = new BufferTag(fileId, pageId);
        BufferSlot bs = store.get(tag);
        if (bs == null) {
            throw new IllegalArgumentException("Page not found in buffer: fileId=" + fileId + ", pageId=" + pageId);
        }
        if (!(bs.getPage() instanceof HeapPage)) {
            throw new IllegalArgumentException("Page is not a HeapPage: " + bs.getPageId());
        }

        Path filePath = getFilePath(fileId);
        pageFileManager.write(bs.getPage(), filePath);
        bs.setDirty(false);
    }

    @Override
    public void flushAllPages() {
        for (BufferSlot bs : getDirtyPages()) {
            if (!(bs.getPage() instanceof HeapPage)) {
                throw new IllegalArgumentException("Dirty page is not a HeapPage: " + bs.getPageId());
            }
            int fileId = bs.getFileId();
            Path filePath = getFilePath(fileId);
            pageFileManager.write(bs.getPage(), filePath);
            bs.setDirty(false);
        }
    }

    @Override
    public List<BufferSlot> getDirtyPages() {
        return store.values().stream().filter(BufferSlot::isDirty).toList();
    }

    /**
     * BufferTag - аналог BufferTag в PostgreSQL
     * Идентифицирует страницу через (fileId, pageId)
     */
    private static class BufferTag {
        private final int fileId;
        private final int pageId;

        public BufferTag(int fileId, int pageId) {
            this.fileId = fileId;
            this.pageId = pageId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BufferTag that = (BufferTag) o;
            return fileId == that.fileId && pageId == that.pageId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileId, pageId);
        }

        @Override
        public String toString() {
            return "BufferTag{fileId=" + fileId + ", pageId=" + pageId + "}";
        }
    }
}