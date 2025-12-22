package ru.open.cu.student.memory.replacer;

import ru.open.cu.student.memory.model.BufferSlot;

import java.util.LinkedHashMap;

public class LRUReplacer implements Replacer {
    private final LinkedHashMap<Integer, BufferSlot> map =
            new LinkedHashMap<>(16, 0.75f, true);

    @Override
    public void push(BufferSlot bufferSlot) {
        map.put(bufferSlot.getPageId(), bufferSlot);
    }

    @Override
    public void delete(int pageId) {
        map.remove(pageId);
    }

    @Override
    public BufferSlot pickVictim() {
        if (map.isEmpty()) return null;

        int lruPageId = map.keySet().iterator().next();
        BufferSlot victim = map.remove(lruPageId);
        return victim;
    }
}
