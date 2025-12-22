package ru.open.cu.student.memory.io;

import ru.open.cu.student.memory.buffer.DefaultBufferPoolManager;

public class DefaultDirtyPageWriter implements DirtyPageWriter {
    private final DefaultBufferPoolManager bm;
    private final int checkpointerInterval = 10000;
    private final int flusherInterval = 1000;
    private final int maxDirtyPages = 100;

    public DefaultDirtyPageWriter(DefaultBufferPoolManager bm) {
        this.bm = bm;
    }

    @Override
    public void startBackgroundWriter() {
        new Thread(() -> {
            while (true) {
                try {
                    var dirtyPages = bm.getDirtyPages();
                    for (int i = 0; i < Math.min(dirtyPages.size(), maxDirtyPages); i++) {
                        bm.flushPage(dirtyPages.get(i).getPageId());
                    }
                    Thread.sleep(flusherInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    @Override
    public void startCheckPointer() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(checkpointerInterval);
                    bm.flushAllPages();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }
}
