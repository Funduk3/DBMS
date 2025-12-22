package ru.open.cu.student.memory.replacer;

import ru.open.cu.student.memory.model.BufferSlot;

import java.util.*;

public class ClockReplacer implements Replacer {
    private final List<BufferSlot> frames;
    private final Map<Integer, Integer> pos;
    private final boolean[] refBits;
    private int hand;
    private final int capacity;

    public ClockReplacer(int capacity) {
        this.capacity = capacity;
        this.frames = new ArrayList<>(capacity);
        this.pos = new HashMap<>();
        this.refBits = new boolean[capacity];
        this.hand = 0;
        for (int i = 0; i < capacity; i++) {
            frames.add(null);
        }
    }

    @Override
    public void push(BufferSlot bufferSlot) {
        Integer existingPos = pos.get(bufferSlot.getPageId());
        if (existingPos != null) {
            refBits[existingPos] = true;
            frames.set(existingPos, bufferSlot);
            return;
        }

        int slotIndex = findEmptySlot();
        if (slotIndex != -1) {
            frames.set(slotIndex, bufferSlot);
            pos.put(bufferSlot.getPageId(), slotIndex);
            refBits[slotIndex] = true;
        } else {
            frames.set(hand, bufferSlot);
            if (frames.get(hand) != null) {
                pos.remove(frames.get(hand).getPageId());
            }
            pos.put(bufferSlot.getPageId(), hand);
            refBits[hand] = true;
            advanceHand();
        }
    }

    private int findEmptySlot() {
        for (int i = 0; i < capacity; i++) {
            if (frames.get(i) == null) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void delete(int pageId) {
        Integer index = pos.remove(pageId);
        if (index != null) {
            frames.set(index, null);
            refBits[index] = false;
        }
    }

    @Override
    public BufferSlot pickVictim() {
        if (frames.stream().allMatch(Objects::isNull)) {
            return null;
        }

        int startHand = hand;
        do {
            BufferSlot currentSlot = frames.get(hand);
            if (currentSlot != null) {
                if (!refBits[hand]) {
                    BufferSlot victim = currentSlot;
                    frames.set(hand, null);
                    pos.remove(victim.getPageId());
                    refBits[hand] = false;
                    advanceHand();
                    return victim;
                }
                refBits[hand] = false;
            }
            advanceHand();
        } while (hand != startHand);

        for (int i = 0; i < capacity; i++) {
            BufferSlot slot = frames.get(i);
            if (slot != null) {
                frames.set(i, null);
                pos.remove(slot.getPageId());
                refBits[i] = false;
                return slot;
            }
        }
        return null;
    }

    private void advanceHand() {
        hand = (hand + 1) % capacity;
    }
}